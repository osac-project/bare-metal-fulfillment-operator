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
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/osac-project/bare-metal-fulfillment-operator/api/v1alpha1"
	"github.com/osac-project/bare-metal-fulfillment-operator/internal/inventory"
	"github.com/osac-project/bare-metal-fulfillment-operator/internal/management"
	"github.com/osac-project/bare-metal-fulfillment-operator/internal/shared"
	opv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac-operator/pkg/provisioning"
)

// HostLeaseReconciler reconciles a HostLease object
type HostLeaseReconciler struct {
	client.Client
	Scheme                            *runtime.Scheme
	InventoryClient                   inventory.Client
	ManagementClient                  management.Client
	ProvisioningProvider              provisioning.ProvisioningProvider
	NoFreeHostsPollIntervalDuration   time.Duration
	TryLockFailPollIntervalDuration   time.Duration
	ManagementRecheckIntervalDuration time.Duration
	ProvisionPollIntervalDuration     time.Duration
}

func NewHostLeaseReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	inventoryClient inventory.Client,
	managementClient management.Client,
	provisioningProvider provisioning.ProvisioningProvider,
	noFreeHostsPollIntervalDuration time.Duration,
	tryLockFailPollIntervalDuration time.Duration,
	managementRecheckIntervalDuration time.Duration,
	provisionPollIntervalDuration time.Duration,
) *HostLeaseReconciler {
	if noFreeHostsPollIntervalDuration <= 0 {
		noFreeHostsPollIntervalDuration = DefaultNoFreeHostsPollIntervalDuration
	}

	if tryLockFailPollIntervalDuration <= 0 {
		tryLockFailPollIntervalDuration = DefaultTryLockFailPollIntervalDuration
	}

	if managementRecheckIntervalDuration <= 0 {
		managementRecheckIntervalDuration = DefaultManagementRecheckIntervalDuration
	}

	if provisionPollIntervalDuration <= 0 {
		provisionPollIntervalDuration = DefaultProvisionPollIntervalDuration
	}

	return &HostLeaseReconciler{
		Client:                            client,
		Scheme:                            scheme,
		InventoryClient:                   inventoryClient,
		NoFreeHostsPollIntervalDuration:   noFreeHostsPollIntervalDuration,
		TryLockFailPollIntervalDuration:   tryLockFailPollIntervalDuration,
		ManagementClient:                  managementClient,
		ProvisioningProvider:              provisioningProvider,
		ManagementRecheckIntervalDuration: managementRecheckIntervalDuration,
		ProvisionPollIntervalDuration:     provisionPollIntervalDuration,
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

	oldstatus := hostLease.Status.DeepCopy()

	var result ctrl.Result
	if !hostLease.DeletionTimestamp.IsZero() {
		result, err = r.handleDeletion(ctx, hostLease)
	} else {
		result, err = r.handleUpdate(ctx, hostLease)
	}

	if !equality.Semantic.DeepEqual(hostLease.Status, *oldstatus) {
		log.Info("Updating HostLease status")
		if statusErr := r.Status().Update(ctx, hostLease); client.IgnoreNotFound(statusErr) != nil {
			return result, statusErr
		}
	}

	log.Info("HostLease reconcile end")
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *HostLeaseReconciler) SetupWithManager(mgr ctrl.Manager, maxConcurrentReconciles int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.HostLease{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles,
		}).
		Named("hostlease").
		Complete(r)
}

// handleUpdate assigns an inventory node to the HostLease CR and marks it as acquired.
func (r *HostLeaseReconciler) handleUpdate(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Updating HostLease")

	if hostLease.Spec.HostClass == "" {
		return r.reconcileInventory(ctx, hostLease)
	}

	return r.reconcileManagement(ctx, hostLease)
}

func (r *HostLeaseReconciler) reconcileInventory(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling inventory")

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
			return ctrl.Result{RequeueAfter: r.NoFreeHostsPollIntervalDuration}, nil
		}

		hostLease.Spec.ExternalHostID = inventoryHost.InventoryHostID
		if err := r.Update(ctx, hostLease); err != nil {
			log.Error(err, "Failed to update HostLease CR with ExternalHostID", "InventoryHostID", inventoryHost.InventoryHostID)
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

	if poolID, ok := hostLease.GetPoolID(); ok {
		combinedLabels[shared.OsacBareMetalPoolIDLabel] = poolID
	}

	inventoryHost, err := r.InventoryClient.AssignHost(
		ctx,
		hostLease.Spec.ExternalHostID,
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
			log.Error(err, "Failed to update HostLease CR")
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
	hostLease.Status.Phase = v1alpha1.HostLeasePhaseProgressing
	hostLease.SetStatusCondition(
		v1alpha1.HostConditionAllocated,
		metav1.ConditionTrue,
		"Allocated",
		fmt.Sprintf("HostLease allocated a host (%s) from %s", hostLease.Spec.ExternalHostID, inventoryHost.HostClass),
	)

	log.Info("Successfully fulfilled HostLease", "InventoryHostID", hostLease.Spec.ExternalHostID)
	return ctrl.Result{}, nil
}

func (r *HostLeaseReconciler) reconcileManagement(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(hostLease, HostLeaseManagementFinalizer) {
		controllerutil.AddFinalizer(hostLease, HostLeaseManagementFinalizer)
		if err := r.Update(ctx, hostLease); err != nil {
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
			return ctrl.Result{}, err
		}
		hostLease.Status.Phase = v1alpha1.HostLeasePhaseProgressing
		return ctrl.Result{}, nil
	}

	// Provisioning runs first — power reconciliation is suspended during provisioning
	if hostLease.Spec.TemplateID != "" && hostLease.Spec.TemplateID != shared.OsacNoopTemplate {
		if r.ProvisioningProvider == nil {
			err := fmt.Errorf("provisioning provider not configured for template %q", hostLease.Spec.TemplateID)
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
			return ctrl.Result{}, err
		}

		result, provErr := r.reconcileProvisioning(ctx, hostLease)
		if provErr != nil {
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
			return result, provErr
		}
		if !result.IsZero() {
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseProgressing
			return result, nil
		}

		provisionCond := hostLease.GetStatusCondition(v1alpha1.HostConditionProvisionTemplateComplete)
		if provisionCond != nil && provisionCond.Status != metav1.ConditionTrue {
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
			log.Info("HostLease not ready: provision template not complete", "hostLease", hostLease.Name)
			return ctrl.Result{}, nil
		}
	}

	powerStatus, err := r.ManagementClient.GetPowerState(ctx, hostLease.Spec.ExternalHostID)
	if err != nil {
		log.Error(err, "failed to get power state", "nodeID", hostLease.Spec.ExternalHostID)
		r.syncHostLeaseStatus(hostLease, nil, err, log)
		return ctrl.Result{}, err
	}
	if powerStatus == nil {
		err := fmt.Errorf("management backend returned nil power status for host %s", hostLease.Spec.ExternalHostID)
		log.Error(err, "unexpected nil power status", "nodeID", hostLease.Spec.ExternalHostID)
		r.syncHostLeaseStatus(hostLease, nil, err, log)
		return ctrl.Result{}, err
	}
	log.V(1).Info("Host power state", "nodeID", hostLease.Spec.ExternalHostID, "power_state", powerStatus.State)

	if hostLease.Spec.PoweredOn != nil {
		if err := r.reconcilePower(ctx, hostLease, powerStatus, log); err != nil {
			r.syncHostLeaseStatus(hostLease, nil, err, log)
			return ctrl.Result{}, err
		}

		powerStatus, err = r.ManagementClient.GetPowerState(ctx, hostLease.Spec.ExternalHostID)
		if err != nil {
			log.Error(err, "failed to refresh power state after reconciliation", "nodeID", hostLease.Spec.ExternalHostID)
			r.syncHostLeaseStatus(hostLease, nil, err, log)
			return ctrl.Result{}, err
		}
		if powerStatus == nil {
			err := fmt.Errorf("management backend returned nil power status for host %s", hostLease.Spec.ExternalHostID)
			log.Error(err, "unexpected nil power status after reconciliation", "nodeID", hostLease.Spec.ExternalHostID)
			r.syncHostLeaseStatus(hostLease, nil, err, log)
			return ctrl.Result{}, err
		}
	}

	r.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

	if hostLease.Spec.PoweredOn != nil {
		if powerStatus.IsTransitioning || *hostLease.Spec.PoweredOn != (powerStatus.State == management.PowerOn) {
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseProgressing
			return ctrl.Result{RequeueAfter: r.ManagementRecheckIntervalDuration}, nil
		}
	}

	hostLease.Status.Phase = v1alpha1.HostLeasePhaseReady
	log.Info("HostLease reconcile completed; status changes pending persistence", "hostLease", hostLease.Name)
	return ctrl.Result{}, nil
}

func (r *HostLeaseReconciler) reconcilePower(ctx context.Context, hostLease *v1alpha1.HostLease, powerStatus *management.PowerStatus, log logr.Logger) error {
	currentlyOn := powerStatus.State == management.PowerOn
	desiredOn := *hostLease.Spec.PoweredOn

	if powerStatus.IsTransitioning {
		log.V(1).Info("Node is transitioning, skipping power action",
			"nodeID", hostLease.Spec.ExternalHostID)
		return nil
	}

	needsPowerUpdate := desiredOn != currentlyOn
	if !needsPowerUpdate {
		log.Info("Power state already matches desired", "poweredOn", desiredOn, "power_state", powerStatus.State)
		return nil
	}

	targetState := management.PowerOff
	action := "off"
	if desiredOn {
		targetState = management.PowerOn
		action = "on"
	}

	log.Info("Powering "+action+" node", "nodeID", hostLease.Spec.ExternalHostID)
	if err := r.ManagementClient.SetPowerState(ctx, hostLease.Spec.ExternalHostID, targetState); err != nil {
		if errors.Is(err, management.ErrTransitioning) {
			log.Info("Node is transitioning (conflict), will retry",
				"nodeID", hostLease.Spec.ExternalHostID)
			return nil
		}
		log.Error(err, "failed to power "+action+" node", "nodeID", hostLease.Spec.ExternalHostID)
		return err
	}

	return nil
}

func (r *HostLeaseReconciler) reconcileProvisioning(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	desiredVersion, err := provisioning.ComputeDesiredConfigVersion(struct {
		HostType                  string
		ExternalHostID            string
		ExternalHostName          string
		HostClass                 string
		NetworkClass              string
		Selector                  v1alpha1.HostSelectorSpec
		InventoryLabels           map[string]string
		InventoryPersistentLabels map[string]string
		TemplateID                string
		TemplateParameters        string
	}{
		hostLease.Spec.HostType,
		hostLease.Spec.ExternalHostID,
		hostLease.Spec.ExternalHostName,
		hostLease.Spec.HostClass,
		hostLease.Spec.NetworkClass,
		hostLease.Spec.Selector,
		hostLease.Spec.InventoryLabels,
		hostLease.Spec.InventoryPersistentLabels,
		hostLease.Spec.TemplateID,
		hostLease.Spec.TemplateParameters,
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to compute desired config version: %w", err)
	}
	hostLease.Status.DesiredConfigVersion = desiredVersion

	result, err := provisioning.RunProvisioningLifecycle(ctx, r.ProvisioningProvider, hostLease,
		&provisioning.State{Jobs: &hostLease.Status.Jobs, DesiredConfigVersion: desiredVersion},
		provisioning.DefaultMaxJobHistory, r.ProvisionPollIntervalDuration,
		&provisioning.PollCallbacks{
			OnFailed: func(message string) {
				hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
				hostLease.SetStatusCondition(
					v1alpha1.HostConditionProvisionTemplateComplete,
					metav1.ConditionFalse,
					v1alpha1.HostConditionReasonTemplateFailed,
					message,
				)
			},
			OnSuccess: func(_ provisioning.ProvisionStatus) {
				hostLease.SetStatusCondition(
					v1alpha1.HostConditionProvisionTemplateComplete,
					metav1.ConditionTrue,
					"Succeeded",
					"Provision job completed successfully",
				)
			},
		},
		func() bool {
			return provisioning.CheckAPIServerForNonTerminalProvisionJob(
				ctx, r.Client, client.ObjectKeyFromObject(hostLease), &v1alpha1.HostLease{},
			)
		},
		func() error {
			return r.Status().Update(ctx, hostLease)
		},
	)
	if err != nil {
		return result, err
	}

	// Set progressing condition while provisioning is in-flight, but don't overwrite a failure.
	provisionCond := hostLease.GetStatusCondition(v1alpha1.HostConditionProvisionTemplateComplete)
	if result.RequeueAfter > 0 && (provisionCond == nil || provisionCond.Reason != v1alpha1.HostConditionReasonTemplateFailed) {
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionProvisionTemplateComplete,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"Provisioning job in progress",
		)
	}

	return result, nil
}

func (r *HostLeaseReconciler) syncHostLeaseStatus(hostLease *v1alpha1.HostLease, powerStatus *management.PowerStatus, reconcileErr error, log logr.Logger) {
	if reconcileErr != nil {
		hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonIronicAPIFailure,
			"failed to sync power status",
		)
		log.Error(reconcileErr, "Failed to sync HostLease power status", "phase", hostLease.Status.Phase, "condition", v1alpha1.HostConditionPowerSynced)
		return
	}

	if powerStatus == nil {
		return
	}

	poweredOn := powerStatus.State == management.PowerOn
	hostLease.Status.PoweredOn = &poweredOn

	if powerStatus.IsTransitioning {
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"node power state is transitioning",
		)
		return
	}

	if hostLease.Spec.PoweredOn != nil && *hostLease.Spec.PoweredOn != poweredOn {
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"waiting for node power state to converge",
		)
	} else if poweredOn {
		hostLease.SetStatusCondition(v1alpha1.HostConditionPowerSynced, metav1.ConditionTrue,
			v1alpha1.HostConditionReasonPowerOn, "")
		log.Info("HostLease power status synced", "poweredOn", poweredOn, "condition", v1alpha1.HostConditionPowerSynced, "conditionStatus", metav1.ConditionTrue, "reason", v1alpha1.HostConditionReasonPowerOn)
	} else {
		hostLease.SetStatusCondition(v1alpha1.HostConditionPowerSynced, metav1.ConditionTrue,
			v1alpha1.HostConditionReasonPowerOff, "")
		log.Info("HostLease power status synced", "poweredOn", poweredOn, "condition", v1alpha1.HostConditionPowerSynced, "conditionStatus", metav1.ConditionTrue, "reason", v1alpha1.HostConditionReasonPowerOff)
	}
}

// handleDeletion frees the host in the inventory and removes the finalizer.
func (r *HostLeaseReconciler) handleDeletion(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Deleting HostLease")

	// Management cleanup
	if controllerutil.ContainsFinalizer(hostLease, HostLeaseManagementFinalizer) {
		log.Info("Running management cleanup", "finalizer", HostLeaseManagementFinalizer)

		hostLease.Status.Phase = v1alpha1.HostLeasePhaseDeleting

		if hostLease.Spec.TemplateID != "" && hostLease.Spec.TemplateID != shared.OsacNoopTemplate {
			if r.ProvisioningProvider == nil {
				err := fmt.Errorf("provisioning provider not configured for template %q", hostLease.Spec.TemplateID)
				return ctrl.Result{}, err
			}

			result, done, err := r.reconcileDeprovisioning(ctx, hostLease)
			if err != nil {
				return result, err
			}
			if !done {
				return result, nil
			}
		}

		controllerutil.RemoveFinalizer(hostLease, HostLeaseManagementFinalizer)
		if err := r.Update(ctx, hostLease); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Management cleanup completed")
	}

	// Inventory cleanup
	if !controllerutil.ContainsFinalizer(hostLease, HostLeaseInventoryFinalizer) {
		log.Info("No inventory finalizer present, deletion complete")
		return ctrl.Result{}, nil
	}

	hostID := hostLease.Spec.ExternalHostID
	if hostID != "" {
		log.Info("Unassigning host from inventory", "InventoryHostID", hostLease.Spec.ExternalHostID)

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

func (r *HostLeaseReconciler) reconcileDeprovisioning(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, bool, error) {
	log := logf.FromContext(ctx)

	if hostLease.Status.Jobs == nil {
		hostLease.Status.Jobs = []opv1alpha1.JobStatus{}
	}

	latestDeprovisionJob := provisioning.FindLatestJobByType(hostLease.Status.Jobs, opv1alpha1.JobTypeDeprovision)

	if !provisioning.HasJobID(latestDeprovisionJob) {
		result, err := provisioning.TriggerDeprovisionJob(
			ctx, r.ProvisioningProvider, hostLease,
			&hostLease.Status.Jobs, provisioning.DefaultMaxJobHistory, r.ProvisionPollIntervalDuration,
		)
		if err != nil {
			log.Error(err, "Failed to trigger deprovision job")
			hostLease.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionFalse,
				v1alpha1.HostConditionReasonTemplateFailed,
				"Failed to trigger deprovision job",
			)
			return result, false, err
		}
		if err := r.Status().Update(ctx, hostLease); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("failed to flush status after deprovision trigger: %w", err)
		}
		if !result.IsZero() {
			hostLease.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionFalse,
				v1alpha1.HostConditionReasonProgressing,
				"Deprovision job in progress",
			)
			return result, false, nil
		}
		return ctrl.Result{}, true, nil
	}

	result, done, err := provisioning.PollDeprovisionJob(
		ctx, r.ProvisioningProvider, hostLease,
		&hostLease.Status.Jobs, latestDeprovisionJob, r.ProvisionPollIntervalDuration,
	)
	if err != nil {
		return result, false, err
	}

	if done {
		if latestDeprovisionJob.State.IsSuccessful() {
			hostLease.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionTrue,
				"Succeeded",
				"Deprovision job completed successfully",
			)
		}
	} else {
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionDeprovisionTemplateComplete,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"Deprovision job in progress",
		)
	}

	return result, done, nil
}
