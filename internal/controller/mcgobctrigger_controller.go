/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	internalv1alpha1 "github.com/functions-dev/mcg-adapter/api/v1alpha1"
	"github.com/functions-dev/mcg-adapter/internal/s3client"
)

// MCGOBCTriggerReconciler reconciles a MCGOBCTrigger object
type MCGOBCTriggerReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	AdapterID    string
	AdapterTopic string
}

// +kubebuilder:rbac:groups=internal.functions.dev,resources=mcgobctriggers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=internal.functions.dev,resources=mcgobctriggers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=internal.functions.dev,resources=mcgobctriggers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *MCGOBCTriggerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var trigger internalv1alpha1.MCGOBCTrigger
	if err := r.Get(ctx, req.NamespacedName, &trigger); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !trigger.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &trigger)
	}

	if !controllerutil.ContainsFinalizer(&trigger, internalv1alpha1.FinalizerName) {
		controllerutil.AddFinalizer(&trigger, internalv1alpha1.FinalizerName)
		if err := r.Update(ctx, &trigger); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	obcName := trigger.Spec.OBC.Name
	ns := trigger.Namespace

	bucketHost, bucketName, bucketPort, err := r.readOBCConfigMap(ctx, ns, obcName)
	if err != nil {
		log.Info("OBC ConfigMap not available, requeuing", "obc", obcName, "error", err)
		r.setCondition(ctx, &trigger, internalv1alpha1.ConditionOBCCredentialsAvailable, metav1.ConditionFalse, "ConfigMapNotReady", err.Error())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	accessKey, secretKey, err := r.readOBCSecret(ctx, ns, obcName)
	if err != nil {
		log.Info("OBC Secret not available, requeuing", "obc", obcName, "error", err)
		r.setCondition(ctx, &trigger, internalv1alpha1.ConditionOBCCredentialsAvailable, metav1.ConditionFalse, "SecretNotReady", err.Error())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	r.setCondition(ctx, &trigger, internalv1alpha1.ConditionOBCCredentialsAvailable, metav1.ConditionTrue, "CredentialsAvailable", "OBC ConfigMap and Secret are available")

	mergedEvents, err := r.computeMergedEvents(ctx, ns, obcName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("computing merged events: %w", err)
	}

	endpoint := fmt.Sprintf("https://%s:%s", bucketHost, bucketPort)
	s3c := s3client.NewS3Client(endpoint, accessKey, secretKey)

	if err := s3client.PutBucketNotification(ctx, s3c, bucketName, r.AdapterID, r.AdapterTopic, mergedEvents); err != nil {
		log.Error(err, "failed to set bucket notification", "bucket", bucketName)
		r.setCondition(ctx, &trigger, internalv1alpha1.ConditionBucketNotificationSet, metav1.ConditionFalse, "PutNotificationFailed", err.Error())
		return ctrl.Result{}, err
	}

	log.Info("bucket notification set", "bucket", bucketName, "events", mergedEvents)
	r.setCondition(ctx, &trigger, internalv1alpha1.ConditionBucketNotificationSet, metav1.ConditionTrue, "NotificationConfigured", "Bucket notification configured successfully")

	return ctrl.Result{}, nil
}

func (r *MCGOBCTriggerReconciler) reconcileDelete(ctx context.Context, trigger *internalv1alpha1.MCGOBCTrigger) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(trigger, internalv1alpha1.FinalizerName) {
		return ctrl.Result{}, nil
	}

	obcName := trigger.Spec.OBC.Name
	ns := trigger.Namespace

	bucketHost, bucketName, bucketPort, err := r.readOBCConfigMap(ctx, ns, obcName)
	if err != nil {
		log.Info("OBC ConfigMap not available during deletion, removing finalizer anyway", "obc", obcName)
	} else {
		accessKey, secretKey, secretErr := r.readOBCSecret(ctx, ns, obcName)
		if secretErr != nil {
			log.Info("OBC Secret not available during deletion, removing finalizer anyway", "obc", obcName)
		} else {
			mergedEvents, mergeErr := r.computeMergedEventsExcluding(ctx, ns, obcName, trigger.Name)
			if mergeErr != nil {
				log.Error(mergeErr, "computing merged events during deletion")
			} else {
				endpoint := fmt.Sprintf("https://%s:%s", bucketHost, bucketPort)
				s3c := s3client.NewS3Client(endpoint, accessKey, secretKey)

				if len(mergedEvents) == 0 {
					if err := s3client.RemoveBucketNotification(ctx, s3c, bucketName); err != nil {
						log.Error(err, "removing bucket notification during deletion", "bucket", bucketName)
					} else {
						log.Info("removed bucket notification", "bucket", bucketName)
					}
				} else {
					if err := s3client.PutBucketNotification(ctx, s3c, bucketName, r.AdapterID, r.AdapterTopic, mergedEvents); err != nil {
						log.Error(err, "updating bucket notification during deletion", "bucket", bucketName)
					} else {
						log.Info("updated bucket notification after deletion", "bucket", bucketName, "events", mergedEvents)
					}
				}
			}
		}
	}

	controllerutil.RemoveFinalizer(trigger, internalv1alpha1.FinalizerName)
	if err := r.Update(ctx, trigger); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MCGOBCTriggerReconciler) readOBCConfigMap(ctx context.Context, namespace, name string) (host, bucketName, port string, err error) {
	var cm corev1.ConfigMap
	if err = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &cm); err != nil {
		return "", "", "", fmt.Errorf("getting ConfigMap %s/%s: %w", namespace, name, err)
	}

	host = cm.Data["BUCKET_HOST"]
	bucketName = cm.Data["BUCKET_NAME"]
	port = cm.Data["BUCKET_PORT"]

	if host == "" || bucketName == "" || port == "" {
		return "", "", "", fmt.Errorf("ConfigMap %s/%s missing required keys (BUCKET_HOST, BUCKET_NAME, BUCKET_PORT)", namespace, name)
	}
	return host, bucketName, port, nil
}

func (r *MCGOBCTriggerReconciler) readOBCSecret(ctx context.Context, namespace, name string) (accessKey, secretKey string, err error) {
	var secret corev1.Secret
	if err = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &secret); err != nil {
		return "", "", fmt.Errorf("getting Secret %s/%s: %w", namespace, name, err)
	}

	accessKey = string(secret.Data["AWS_ACCESS_KEY_ID"])
	secretKey = string(secret.Data["AWS_SECRET_ACCESS_KEY"])

	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("Secret %s/%s missing required keys (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)", namespace, name)
	}
	return accessKey, secretKey, nil
}

func (r *MCGOBCTriggerReconciler) computeMergedEvents(ctx context.Context, namespace, obcName string) ([]string, error) {
	return r.computeMergedEventsExcluding(ctx, namespace, obcName, "")
}

func (r *MCGOBCTriggerReconciler) computeMergedEventsExcluding(ctx context.Context, namespace, obcName, excludeName string) ([]string, error) {
	var list internalv1alpha1.MCGOBCTriggerList
	if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	eventSet := make(map[string]struct{})
	for _, t := range list.Items {
		if t.Spec.OBC.Name != obcName {
			continue
		}
		if excludeName != "" && t.Name == excludeName {
			continue
		}
		if !t.DeletionTimestamp.IsZero() && t.Name != excludeName {
			continue
		}
		for _, e := range t.Spec.Events {
			eventSet[e] = struct{}{}
		}
	}

	events := make([]string, 0, len(eventSet))
	for e := range eventSet {
		events = append(events, e)
	}
	return events, nil
}

func (r *MCGOBCTriggerReconciler) setCondition(ctx context.Context, trigger *internalv1alpha1.MCGOBCTrigger, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&trigger.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: trigger.Generation,
	})
	if err := r.Status().Update(ctx, trigger); err != nil {
		logf.FromContext(ctx).Error(err, "updating status condition", "type", condType)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *MCGOBCTriggerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&internalv1alpha1.MCGOBCTrigger{}).
		Named("mcgobctrigger").
		Complete(r)
}
