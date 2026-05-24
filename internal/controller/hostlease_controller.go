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
	"maps"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/osac-project/bare-metal-fulfillment-operator/api/v1alpha1"
	"github.com/osac-project/bare-metal-fulfillment-operator/internal/inventory"
)

// HostLeaseReconciler reconciles a HostLease object
type HostLeaseReconciler struct {
	client.Client
	Scheme                          *runtime.Scheme
	InventoryClient                 inventory.Client
	NoFreeHostsPollIntervalDuration time.Duration
	TryLockFailPollIntervalDuration time.Duration
}

func NewHostLeaseReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	inventoryClient inventory.Client,
	noFreeHostsPollIntervalDuration time.Duration,
	tryLockFailPollIntervalDuration time.Duration,
) *HostLeaseReconciler {
	if noFreeHostsPollIntervalDuration <= 0 {
		noFreeHostsPollIntervalDuration = DefaultNoFreeHostsPollIntervalDuration
	}

	if tryLockFailPollIntervalDuration <= 0 {
		tryLockFailPollIntervalDuration = DefaultTryLockFailPollIntervalDuration
	}

	return &HostLeaseReconciler{
		Client:                          client,
		Scheme:                          scheme,
		InventoryClient:                 inventoryClient,
		NoFreeHostsPollIntervalDuration: noFreeHostsPollIntervalDuration,
		TryLockFailPollIntervalDuration: tryLockFailPollIntervalDuration,
	}
}

// +kubebuilder:rbac:groups=osac.openshift.io,resources=hostleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=osac.openshift.io,resources=hostleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=osac.openshift.io,resources=hostleases/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the pool closer to the desired state.
func (r *HostLeaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("HostLease reconcile start")

	hostLease := &v1alpha1.HostLease{}
	err := r.Get(ctx, req.NamespacedName, hostLease)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var result ctrl.Result
	if !hostLease.DeletionTimestamp.IsZero() {
		result, err = r.handleDeletion(ctx, hostLease)
	} else {
		result, err = r.handleUpdate(ctx, hostLease)
	}

	log.Info("HostLease reconcile end")
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *HostLeaseReconciler) SetupWithManager(mgr ctrl.Manager, maxConcurrentReconciles int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.HostLease{}).
		Named("hostlease").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles,
		}).
		WithEventFilter(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				newHostLease := e.ObjectNew.(*v1alpha1.HostLease)
				return newHostLease.Spec.HostClass == ""
			},
		}).
		Complete(r)
}

// handleUpdate assigns an inventory node to the HostLease CR and marks it as acquired.
func (r *HostLeaseReconciler) handleUpdate(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Updating HostLease")

	hostLease.Status.Phase = v1alpha1.HostLeasePhaseAllocating
	hostLease.SetStatusCondition(
		v1alpha1.HostConditionAllocated,
		metav1.ConditionFalse,
		v1alpha1.HostConditionReasonProgressing,
		"Allocating HostLease",
	)

	if controllerutil.AddFinalizer(hostLease, HostLeaseInventoryFinalizer) {
		if err := r.Update(ctx, hostLease); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		log.Info("Added finalizer")
		if err := r.Status().Update(ctx, hostLease); err != nil {
			log.Error(err, "Failed to update HostLease status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	poolID, ok := hostLease.GetPoolID()
	if !ok {
		log.Info("HostLease is orphaned so delete it")
		if err := r.Delete(ctx, hostLease); err != nil {
			log.Error(err, "Failed to delete HostLease")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if hostLease.Spec.ExternalHostID == "" {
		matchExpressions := maps.Clone(hostLease.Spec.Selector.HostSelector)
		if matchExpressions == nil {
			matchExpressions = map[string]string{}
		}
		matchExpressions["hostType"] = hostLease.Spec.HostType

		inventoryHost, err := r.InventoryClient.FindFreeHost(ctx, matchExpressions)
		if err != nil {
			log.Error(err, "Failed to find a free host", "matchExpressions", matchExpressions)
			return ctrl.Result{}, err
		}
		if inventoryHost == nil {
			log.Info("No matching hosts available", "matchExpressions", matchExpressions)
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
			hostLease.SetStatusCondition(
				v1alpha1.HostConditionAllocated,
				metav1.ConditionFalse,
				"Failed",
				"No matching hosts available",
			)
			if err := r.Status().Update(ctx, hostLease); err != nil {
				log.Error(err, "Failed to update HostLease status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: r.NoFreeHostsPollIntervalDuration}, nil
		}

		hostLease.Spec.ExternalHostID = inventoryHost.InventoryHostID
		if err := r.Update(ctx, hostLease); err != nil {
			log.Error(err, "Failed to update HostLease CR with ExternalHostID", "InventoryHostID", inventoryHost.InventoryHostID)
			return ctrl.Result{}, err
		}
		if err := r.Status().Update(ctx, hostLease); err != nil {
			log.Error(err, "Failed to update HostLease status")
			return ctrl.Result{}, err
		}

		log.Info("Successfully updated HostLease with inventory host id")
		return ctrl.Result{}, nil
	}

	hostID := hostLease.Spec.ExternalHostID
	if !inventory.TryLock(hostID) {
		log.Info("Lock is currently held, retrying", "InventoryHostID", hostID)
		return ctrl.Result{RequeueAfter: r.TryLockFailPollIntervalDuration}, nil
	}
	defer inventory.Unlock(hostID)

	// Build combined labels from spec fields
	// Persistent labels override non-persistent labels with the same key
	combinedLabels := make(map[string]string)

	// First, copy inventory labels
	if hostLease.Spec.InventoryLabels != nil {
		maps.Copy(combinedLabels, hostLease.Spec.InventoryLabels)
	}

	// Then copy persistent labels (will override any duplicates)
	if hostLease.Spec.InventoryPersistentLabels != nil {
		maps.Copy(combinedLabels, hostLease.Spec.InventoryPersistentLabels)
	}

	inventoryHost, err := r.InventoryClient.AssignHost(
		ctx,
		hostLease.Spec.ExternalHostID,
		poolID,
		string(hostLease.UID),
		combinedLabels,
	)
	if err != nil {
		log.Error(err, "Failed to assign host", "InventoryHostID", hostLease.Spec.ExternalHostID)
		return ctrl.Result{}, err
	}
	if inventoryHost == nil {
		log.Info("Host is acquired by a different HostLease, unsetting ExternalHostID", "InventoryHostID", hostID)
		hostLease.Spec.ExternalHostID = ""
		if err = r.Update(ctx, hostLease); err != nil {
			log.Error(err, "Failed to delete HostLease CR")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	hostLease.Spec.HostClass = inventoryHost.HostClass
	hostLease.Spec.NetworkClass = inventoryHost.NetworkClass
	if err = r.Update(ctx, hostLease); err != nil {
		log.Error(err, "Failed to update HostLease CR with HostClass", "HostClass", inventoryHost.HostClass)
		return ctrl.Result{}, err
	}

	// Update status to indicate successful allocation
	hostLease.Status.Phase = v1alpha1.HostLeasePhaseReady
	hostLease.SetStatusCondition(
		v1alpha1.HostConditionAllocated,
		metav1.ConditionTrue,
		"Allocated",
		fmt.Sprintf("HostLease allocated a host (%s) from %s", hostLease.Spec.ExternalHostID, inventoryHost.HostClass),
	)
	if err = r.Status().Update(ctx, hostLease); err != nil {
		log.Error(err, "Failed to update HostLease status")
		return ctrl.Result{}, err
	}

	log.Info("Successfully fulfilled HostLease", "InventoryHostID", hostLease.Spec.ExternalHostID)
	return ctrl.Result{}, nil
}

// handleDeletion frees the host in the inventory and removes the finalizer.
func (r *HostLeaseReconciler) handleDeletion(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Deleting HostLease")

	// Only free in inventory if an inventory host is marked
	if hostLease.Spec.ExternalHostID != "" {
		log.Info("Unassigning host from inventory", "InventoryHostID", hostLease.Spec.ExternalHostID)

		hostID := hostLease.Spec.ExternalHostID
		if !inventory.TryLock(hostID) {
			log.Info("Could not acquire lock for host", "InventoryHostID", hostID)
			return ctrl.Result{RequeueAfter: r.TryLockFailPollIntervalDuration}, nil
		}
		defer inventory.Unlock(hostID)

		// Collect non-persistent inventory labels to remove
		var labelsToRemove []string
		if hostLease.Spec.InventoryLabels != nil {
			labelsToRemove = make([]string, 0, len(hostLease.Spec.InventoryLabels))
			for key := range hostLease.Spec.InventoryLabels {
				labelsToRemove = append(labelsToRemove, key)
			}
		}

		err := r.InventoryClient.UnassignHost(ctx, hostID, labelsToRemove)
		if err != nil {
			log.Error(err, "Failed to unassign host in inventory")
			return ctrl.Result{}, err
		}
	}

	if controllerutil.RemoveFinalizer(hostLease, HostLeaseInventoryFinalizer) {
		if err := r.Update(ctx, hostLease); err != nil {
			log.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
	}

	log.Info("Successfully un-fulfilled HostLease")
	return ctrl.Result{}, nil
}
