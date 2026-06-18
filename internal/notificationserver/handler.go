package notificationserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	internalv1alpha1 "github.com/functions-dev/mcg-adapter/api/v1alpha1"
	ceDispatch "github.com/functions-dev/mcg-adapter/internal/cloudevents"
	"github.com/functions-dev/mcg-adapter/internal/eventmatch"
)

type notificationHandler struct {
	client client.Client
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
	defer r.Body.Close()

	var payload notificationPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		log.Error(err, "parsing notification payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	for _, rawRecord := range payload.Records {
		var header recordHeader
		if err := json.Unmarshal(rawRecord, &header); err != nil {
			log.Error(err, "parsing record header")
			continue
		}

		if header.Event == "s3:TestEvent" {
			h.handleTestEvent(ctx, header.Bucket)
		} else if header.EventName != "" && header.S3 != nil {
			h.handleDataEvent(ctx, header.S3.Bucket.Name, header.EventName, rawRecord)
		}
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

func (h *notificationHandler) handleDataEvent(ctx context.Context, bucketName, eventName string, rawRecord json.RawMessage) {
	triggers, err := h.findTriggersForBucket(ctx, bucketName)
	if err != nil {
		log.Error(err, "finding triggers for data event", "bucket", bucketName, "event", eventName)
		return
	}

	for _, trigger := range triggers {
		matched := false
		for _, pattern := range trigger.Spec.Events {
			if eventmatch.MatchEvent(pattern, eventName) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		for _, target := range trigger.Spec.Triggers {
			if err := ceDispatch.DispatchEvent(ctx, target.URI, bucketName, eventName, rawRecord); err != nil {
				log.Error(err, "dispatching event", "target", target.URI, "bucket", bucketName, "event", eventName)
			} else {
				log.Info("dispatched event", "target", target.URI, "bucket", bucketName, "event", eventName)
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
