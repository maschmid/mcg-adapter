package notificationserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/IBM/sarama"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	internalv1alpha1 "github.com/functions-dev/mcg-adapter/api/v1alpha1"
	ceDispatch "github.com/functions-dev/mcg-adapter/internal/cloudevents"
	"github.com/functions-dev/mcg-adapter/internal/eventmatch"
)

type notificationHandler struct {
	client        client.Client
	kafkaProducer sarama.SyncProducer
}

type notificationPayload struct {
	Records []json.RawMessage `json:"Records"`
}

type recordHeader struct {
	Event     string   `json:"Event,omitempty"`
	EventName string   `json:"eventName,omitempty"`
	Bucket    string   `json:"Bucket,omitempty"`
	S3        *s3Field `json:"s3,omitempty"`
}

type s3Field struct {
	Bucket s3Bucket `json:"bucket"`
}

type s3Bucket struct {
	Name string `json:"name"`
}

type parsedRecord struct {
	raw    json.RawMessage
	header recordHeader
}

func (h *notificationHandler) handleNotification(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error(err, "reading request body")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	var payload notificationPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		log.Error(err, "parsing notification payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	var dataRecords []parsedRecord
	for _, rawRecord := range payload.Records {
		var header recordHeader
		if err := json.Unmarshal(rawRecord, &header); err != nil {
			log.Error(err, "parsing record header")
			continue
		}

		if header.Event == "s3:TestEvent" {
			h.handleTestEvent(ctx, header.Bucket)
		} else if header.EventName != "" && header.S3 != nil {
			dataRecords = append(dataRecords, parsedRecord{raw: rawRecord, header: header})
		}
	}

	if len(dataRecords) > 0 {
		h.dispatchDataEvents(ctx, dataRecords)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *notificationHandler) handleTestEvent(ctx context.Context, bucketName string) {
	triggers, err := h.findTriggersForBucket(ctx, bucketName)
	if err != nil {
		log.Error(err, "finding triggers for test event", "bucket", bucketName)
		return
	}

	for i := range triggers {
		trigger := &triggers[i]
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := h.client.Get(ctx, client.ObjectKeyFromObject(trigger), trigger); err != nil {
				return err
			}
			meta.SetStatusCondition(&trigger.Status.Conditions, metav1.Condition{
				Type:               internalv1alpha1.ConditionTestEventReceived,
				Status:             metav1.ConditionTrue,
				Reason:             "TestEventReceived",
				Message:            "Received test event from NooBaa",
				ObservedGeneration: trigger.Generation,
			})
			return h.client.Status().Update(ctx, trigger)
		}); err != nil {
			log.Error(err, "updating TestEventReceived condition", "trigger", trigger.Name, "namespace", trigger.Namespace)
		} else {
			log.Info("set TestEventReceived=True", "trigger", trigger.Name, "namespace", trigger.Namespace)
		}
	}
}

func (h *notificationHandler) dispatchDataEvents(ctx context.Context, records []parsedRecord) {
	var allTriggers internalv1alpha1.MCGOBCTriggerList
	if err := h.client.List(ctx, &allTriggers); err != nil {
		log.Error(err, "listing triggers for data events")
		return
	}

	for _, trigger := range allTriggers.Items {
		var matchedRecords []json.RawMessage
		for _, rec := range records {
			if rec.header.S3.Bucket.Name != trigger.Spec.OBC.Name {
				continue
			}
			for _, pattern := range trigger.Spec.Events {
				if eventmatch.MatchEvent(pattern, rec.header.EventName) {
					matchedRecords = append(matchedRecords, rec.raw)
					break
				}
			}
		}

		if len(matchedRecords) == 0 {
			continue
		}

		for _, target := range trigger.Spec.Triggers {
			if target.URI != "" {
				if err := ceDispatch.DispatchEvent(ctx, target.URI, trigger.Spec.OBC.Name, matchedRecords); err != nil {
					log.Error(err, "dispatching event", "target", target.URI, "bucket", trigger.Spec.OBC.Name)
				} else {
					log.Info("dispatched event", "target", target.URI, "bucket", trigger.Spec.OBC.Name, "records", len(matchedRecords))
				}
			}
			if target.Kafka != nil {
				if h.kafkaProducer == nil {
					log.Error(fmt.Errorf("no kafka brokers configured"), "cannot dispatch to kafka", "topic", target.Kafka.Topic, "bucket", trigger.Spec.OBC.Name)
					continue
				}
				if err := ceDispatch.DispatchEventToKafka(ctx, h.kafkaProducer, target.Kafka.Topic, trigger.Spec.OBC.Name, matchedRecords); err != nil {
					log.Error(err, "dispatching event to kafka", "topic", target.Kafka.Topic, "bucket", trigger.Spec.OBC.Name)
				} else {
					log.Info("dispatched event to kafka", "topic", target.Kafka.Topic, "bucket", trigger.Spec.OBC.Name, "records", len(matchedRecords))
				}
			}
		}
	}
}

func (h *notificationHandler) findTriggersForBucket(ctx context.Context, bucketName string) ([]internalv1alpha1.MCGOBCTrigger, error) {
	var allTriggers internalv1alpha1.MCGOBCTriggerList
	if err := h.client.List(ctx, &allTriggers); err != nil {
		return nil, err
	}

	var matched []internalv1alpha1.MCGOBCTrigger
	for _, t := range allTriggers.Items {
		if t.Spec.OBC.Name == bucketName {
			matched = append(matched, t)
		}
	}
	return matched, nil
}
