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

// BareMetalInstanceReconciler reconciles a BareMetalInstance object
type BareMetalInstanceReconciler struct {
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

func NewBareMetalInstanceReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	inventoryClient inventory.Client,
	managementClient management.Client,
	provisioningProvider provisioning.ProvisioningProvider,
	noFreeHostsPollIntervalDuration time.Duration,
	tryLockFailPollIntervalDuration time.Duration,
	managementRecheckIntervalDuration time.Duration,
	provisionPollIntervalDuration time.Duration,
) *BareMetalInstanceReconciler {
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

	return &BareMetalInstanceReconciler{
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

// +kubebuilder:rbac:groups=osac.openshift.io,resources=baremetalinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=osac.openshift.io,resources=baremetalinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=osac.openshift.io,resources=baremetalinstances/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the pool closer to the desired state.
func (r *BareMetalInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("BareMetalInstance reconcile start")

	bareMetalInstance := &v1alpha1.BareMetalInstance{}
	err := r.Get(ctx, req.NamespacedName, bareMetalInstance)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	oldstatus := bareMetalInstance.Status.DeepCopy()

	var result ctrl.Result
	if !bareMetalInstance.DeletionTimestamp.IsZero() {
		result, err = r.handleDeletion(ctx, bareMetalInstance)
	} else {
		result, err = r.handleUpdate(ctx, bareMetalInstance)
	}

	if !equality.Semantic.DeepEqual(bareMetalInstance.Status, *oldstatus) {
		log.Info("Updating BareMetalInstance status")
		if statusErr := r.Status().Update(ctx, bareMetalInstance); client.IgnoreNotFound(statusErr) != nil {
			return result, statusErr
		}
	}

	log.Info("BareMetalInstance reconcile end")
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *BareMetalInstanceReconciler) SetupWithManager(mgr ctrl.Manager, maxConcurrentReconciles int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.BareMetalInstance{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles,
		}).
		Named("baremetalinstance").
		Complete(r)
}

// handleUpdate assigns an inventory node to the BareMetalInstance CR and marks it as acquired.
func (r *BareMetalInstanceReconciler) handleUpdate(ctx context.Context, bareMetalInstance *v1alpha1.BareMetalInstance) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Updating BareMetalInstance")

	if bareMetalInstance.Spec.HostClass == "" {
		return r.reconcileInventory(ctx, bareMetalInstance)
	}

	return r.reconcileManagement(ctx, bareMetalInstance)
}

func (r *BareMetalInstanceReconciler) reconcileInventory(ctx context.Context, bareMetalInstance *v1alpha1.BareMetalInstance) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling inventory")

	bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseAllocating
	bareMetalInstance.SetStatusCondition(
		v1alpha1.HostConditionAllocated,
		metav1.ConditionFalse,
		v1alpha1.HostConditionReasonProgressing,
		"Allocating BareMetalInstance",
	)

	if controllerutil.AddFinalizer(bareMetalInstance, BareMetalInstanceInventoryFinalizer) {
		if err := r.Update(ctx, bareMetalInstance); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		log.Info("Added finalizer")
		return ctrl.Result{}, nil
	}

	if bareMetalInstance.Spec.ExternalHostID == "" {
		matchExpressions := maps.Clone(bareMetalInstance.Spec.Selector.HostSelector)
		if matchExpressions == nil {
			matchExpressions = map[string]string{}
		}
		matchExpressions["hostType"] = bareMetalInstance.Spec.HostType

		inventoryHost, err := r.InventoryClient.FindFreeHost(ctx, matchExpressions)
		if err != nil {
			log.Error(err, "Failed to find a free host", "matchExpressions", matchExpressions)
			return ctrl.Result{}, err
		}
		if inventoryHost == nil {
			log.Info("No matching hosts available", "matchExpressions", matchExpressions)
			bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseFailed
			bareMetalInstance.SetStatusCondition(
				v1alpha1.HostConditionAllocated,
				metav1.ConditionFalse,
				"Failed",
				"No matching hosts available",
			)
			return ctrl.Result{RequeueAfter: r.NoFreeHostsPollIntervalDuration}, nil
		}

		bareMetalInstance.Spec.ExternalHostID = inventoryHost.InventoryHostID
		if err := r.Update(ctx, bareMetalInstance); err != nil {
			log.Error(err, "Failed to update BareMetalInstance CR with ExternalHostID", "InventoryHostID", inventoryHost.InventoryHostID)
			return ctrl.Result{}, err
		}

		log.Info("Successfully updated BareMetalInstance with inventory host id")
		return ctrl.Result{}, nil
	}

	hostID := bareMetalInstance.Spec.ExternalHostID
	if !inventory.TryLock(hostID) {
		log.Info("Lock is currently held, retrying", "InventoryHostID", hostID)
		return ctrl.Result{RequeueAfter: r.TryLockFailPollIntervalDuration}, nil
	}
	defer inventory.Unlock(hostID)

	// Build combined labels from spec fields
	// Persistent labels override non-persistent labels with the same key
	combinedLabels := make(map[string]string)

	// First, copy inventory labels
	if bareMetalInstance.Spec.InventoryLabels != nil {
		maps.Copy(combinedLabels, bareMetalInstance.Spec.InventoryLabels)
	}

	// Then copy persistent labels (will override any duplicates)
	if bareMetalInstance.Spec.InventoryPersistentLabels != nil {
		maps.Copy(combinedLabels, bareMetalInstance.Spec.InventoryPersistentLabels)
	}

	if poolID, ok := bareMetalInstance.GetPoolID(); ok {
		combinedLabels[shared.OsacBareMetalPoolIDLabel] = poolID
	}

	inventoryHost, err := r.InventoryClient.AssignHost(
		ctx,
		bareMetalInstance.Spec.ExternalHostID,
		string(bareMetalInstance.UID),
		combinedLabels,
	)
	if err != nil {
		log.Error(err, "Failed to assign host", "InventoryHostID", bareMetalInstance.Spec.ExternalHostID)
		return ctrl.Result{}, err
	}
	if inventoryHost == nil {
		log.Info("Host is acquired by a different BareMetalInstance, unsetting ExternalHostID", "InventoryHostID", hostID)
		bareMetalInstance.Spec.ExternalHostID = ""
		if err = r.Update(ctx, bareMetalInstance); err != nil {
			log.Error(err, "Failed to update BareMetalInstance CR")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	bareMetalInstance.Spec.HostClass = inventoryHost.HostClass
	bareMetalInstance.Spec.NetworkClass = inventoryHost.NetworkClass
	if err = r.Update(ctx, bareMetalInstance); err != nil {
		log.Error(err, "Failed to update BareMetalInstance CR with HostClass", "HostClass", inventoryHost.HostClass)
		return ctrl.Result{}, err
	}

	// Update status to indicate successful allocation
	bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseProgressing
	bareMetalInstance.SetStatusCondition(
		v1alpha1.HostConditionAllocated,
		metav1.ConditionTrue,
		"Allocated",
		fmt.Sprintf("BareMetalInstance allocated a host (%s) from %s", bareMetalInstance.Spec.ExternalHostID, inventoryHost.HostClass),
	)

	log.Info("Successfully fulfilled BareMetalInstance", "InventoryHostID", bareMetalInstance.Spec.ExternalHostID)
	return ctrl.Result{}, nil
}

func (r *BareMetalInstanceReconciler) reconcileManagement(ctx context.Context, bareMetalInstance *v1alpha1.BareMetalInstance) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(bareMetalInstance, BareMetalInstanceManagementFinalizer) {
		controllerutil.AddFinalizer(bareMetalInstance, BareMetalInstanceManagementFinalizer)
		if err := r.Update(ctx, bareMetalInstance); err != nil {
			bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseFailed
			return ctrl.Result{}, err
		}
		bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseProgressing
		return ctrl.Result{}, nil
	}

	// Provisioning runs first — power reconciliation is suspended during provisioning
	if bareMetalInstance.Spec.TemplateID != "" && bareMetalInstance.Spec.TemplateID != shared.OsacNoopTemplate {
		if r.ProvisioningProvider == nil {
			err := fmt.Errorf("provisioning provider not configured for template %q", bareMetalInstance.Spec.TemplateID)
			bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseFailed
			return ctrl.Result{}, err
		}

		result, provErr := r.reconcileProvisioning(ctx, bareMetalInstance)
		if provErr != nil {
			bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseFailed
			return result, provErr
		}
		if !result.IsZero() {
			bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseProgressing
			return result, nil
		}

		provisionCond := bareMetalInstance.GetStatusCondition(v1alpha1.HostConditionProvisionTemplateComplete)
		if provisionCond != nil && provisionCond.Status != metav1.ConditionTrue {
			bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseFailed
			log.Info("BareMetalInstance not ready: provision template not complete", "bareMetalInstance", bareMetalInstance.Name)
			return ctrl.Result{}, nil
		}
	}

	powerStatus, err := r.ManagementClient.GetPowerState(ctx, bareMetalInstance.Spec.ExternalHostID)
	if err != nil {
		log.Error(err, "failed to get power state", "nodeID", bareMetalInstance.Spec.ExternalHostID)
		r.syncBareMetalInstanceStatus(bareMetalInstance, nil, err, log)
		return ctrl.Result{}, err
	}
	if powerStatus == nil {
		err := fmt.Errorf("management backend returned nil power status for host %s", bareMetalInstance.Spec.ExternalHostID)
		log.Error(err, "unexpected nil power status", "nodeID", bareMetalInstance.Spec.ExternalHostID)
		r.syncBareMetalInstanceStatus(bareMetalInstance, nil, err, log)
		return ctrl.Result{}, err
	}
	log.V(1).Info("Host power state", "nodeID", bareMetalInstance.Spec.ExternalHostID, "power_state", powerStatus.State)

	if bareMetalInstance.Spec.PoweredOn != nil {
		if err := r.reconcilePower(ctx, bareMetalInstance, powerStatus, log); err != nil {
			r.syncBareMetalInstanceStatus(bareMetalInstance, nil, err, log)
			return ctrl.Result{}, err
		}

		powerStatus, err = r.ManagementClient.GetPowerState(ctx, bareMetalInstance.Spec.ExternalHostID)
		if err != nil {
			log.Error(err, "failed to refresh power state after reconciliation", "nodeID", bareMetalInstance.Spec.ExternalHostID)
			r.syncBareMetalInstanceStatus(bareMetalInstance, nil, err, log)
			return ctrl.Result{}, err
		}
		if powerStatus == nil {
			err := fmt.Errorf("management backend returned nil power status for host %s", bareMetalInstance.Spec.ExternalHostID)
			log.Error(err, "unexpected nil power status after reconciliation", "nodeID", bareMetalInstance.Spec.ExternalHostID)
			r.syncBareMetalInstanceStatus(bareMetalInstance, nil, err, log)
			return ctrl.Result{}, err
		}
	}

	r.syncBareMetalInstanceStatus(bareMetalInstance, powerStatus, nil, log)

	if bareMetalInstance.Spec.PoweredOn != nil {
		if powerStatus.IsTransitioning || *bareMetalInstance.Spec.PoweredOn != (powerStatus.State == management.PowerOn) {
			bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseProgressing
			return ctrl.Result{RequeueAfter: r.ManagementRecheckIntervalDuration}, nil
		}
	}

	bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseReady
	log.Info("BareMetalInstance reconcile completed; status changes pending persistence", "bareMetalInstance", bareMetalInstance.Name)
	return ctrl.Result{}, nil
}

func (r *BareMetalInstanceReconciler) reconcilePower(ctx context.Context, bareMetalInstance *v1alpha1.BareMetalInstance, powerStatus *management.PowerStatus, log logr.Logger) error {
	currentlyOn := powerStatus.State == management.PowerOn
	desiredOn := *bareMetalInstance.Spec.PoweredOn

	if powerStatus.IsTransitioning {
		log.V(1).Info("Node is transitioning, skipping power action",
			"nodeID", bareMetalInstance.Spec.ExternalHostID)
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

	log.Info("Powering "+action+" node", "nodeID", bareMetalInstance.Spec.ExternalHostID)
	if err := r.ManagementClient.SetPowerState(ctx, bareMetalInstance.Spec.ExternalHostID, targetState); err != nil {
		if errors.Is(err, management.ErrTransitioning) {
			log.Info("Node is transitioning (conflict), will retry",
				"nodeID", bareMetalInstance.Spec.ExternalHostID)
			return nil
		}
		log.Error(err, "failed to power "+action+" node", "nodeID", bareMetalInstance.Spec.ExternalHostID)
		return err
	}

	return nil
}

func (r *BareMetalInstanceReconciler) reconcileProvisioning(ctx context.Context, bareMetalInstance *v1alpha1.BareMetalInstance) (ctrl.Result, error) {
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
		bareMetalInstance.Spec.HostType,
		bareMetalInstance.Spec.ExternalHostID,
		bareMetalInstance.Spec.ExternalHostName,
		bareMetalInstance.Spec.HostClass,
		bareMetalInstance.Spec.NetworkClass,
		bareMetalInstance.Spec.Selector,
		bareMetalInstance.Spec.InventoryLabels,
		bareMetalInstance.Spec.InventoryPersistentLabels,
		bareMetalInstance.Spec.TemplateID,
		bareMetalInstance.Spec.TemplateParameters,
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to compute desired config version: %w", err)
	}
	bareMetalInstance.Status.DesiredConfigVersion = desiredVersion

	result, err := provisioning.RunProvisioningLifecycle(ctx, r.ProvisioningProvider, bareMetalInstance,
		&provisioning.State{Jobs: &bareMetalInstance.Status.Jobs, DesiredConfigVersion: desiredVersion},
		provisioning.DefaultMaxJobHistory, r.ProvisionPollIntervalDuration,
		&provisioning.PollCallbacks{
			OnFailed: func(message string) {
				bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseFailed
				bareMetalInstance.SetStatusCondition(
					v1alpha1.HostConditionProvisionTemplateComplete,
					metav1.ConditionFalse,
					v1alpha1.HostConditionReasonTemplateFailed,
					message,
				)
			},
			OnSuccess: func(_ provisioning.ProvisionStatus) {
				bareMetalInstance.SetStatusCondition(
					v1alpha1.HostConditionProvisionTemplateComplete,
					metav1.ConditionTrue,
					"Succeeded",
					"Provision job completed successfully",
				)
			},
		},
		func() bool {
			return provisioning.CheckAPIServerForNonTerminalProvisionJob(
				ctx, r.Client, client.ObjectKeyFromObject(bareMetalInstance), &v1alpha1.BareMetalInstance{},
			)
		},
		func() error {
			return r.Status().Update(ctx, bareMetalInstance)
		},
	)
	if err != nil {
		return result, err
	}

	// Set progressing condition while provisioning is in-flight, but don't overwrite a failure.
	provisionCond := bareMetalInstance.GetStatusCondition(v1alpha1.HostConditionProvisionTemplateComplete)
	if result.RequeueAfter > 0 && (provisionCond == nil || provisionCond.Reason != v1alpha1.HostConditionReasonTemplateFailed) {
		bareMetalInstance.SetStatusCondition(
			v1alpha1.HostConditionProvisionTemplateComplete,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"Provisioning job in progress",
		)
	}

	return result, nil
}

func (r *BareMetalInstanceReconciler) syncBareMetalInstanceStatus(bareMetalInstance *v1alpha1.BareMetalInstance, powerStatus *management.PowerStatus, reconcileErr error, log logr.Logger) {
	if reconcileErr != nil {
		bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseFailed
		bareMetalInstance.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonIronicAPIFailure,
			"failed to sync power status",
		)
		log.Error(reconcileErr, "Failed to sync BareMetalInstance power status", "phase", bareMetalInstance.Status.Phase, "condition", v1alpha1.HostConditionPowerSynced)
		return
	}

	if powerStatus == nil {
		return
	}

	poweredOn := powerStatus.State == management.PowerOn
	bareMetalInstance.Status.PoweredOn = &poweredOn

	if powerStatus.IsTransitioning {
		bareMetalInstance.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"node power state is transitioning",
		)
		return
	}

	if bareMetalInstance.Spec.PoweredOn != nil && *bareMetalInstance.Spec.PoweredOn != poweredOn {
		bareMetalInstance.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"waiting for node power state to converge",
		)
	} else if poweredOn {
		bareMetalInstance.SetStatusCondition(v1alpha1.HostConditionPowerSynced, metav1.ConditionTrue,
			v1alpha1.HostConditionReasonPowerOn, "")
		log.Info("BareMetalInstance power status synced", "poweredOn", poweredOn, "condition", v1alpha1.HostConditionPowerSynced, "conditionStatus", metav1.ConditionTrue, "reason", v1alpha1.HostConditionReasonPowerOn)
	} else {
		bareMetalInstance.SetStatusCondition(v1alpha1.HostConditionPowerSynced, metav1.ConditionTrue,
			v1alpha1.HostConditionReasonPowerOff, "")
		log.Info("BareMetalInstance power status synced", "poweredOn", poweredOn, "condition", v1alpha1.HostConditionPowerSynced, "conditionStatus", metav1.ConditionTrue, "reason", v1alpha1.HostConditionReasonPowerOff)
	}
}

// handleDeletion frees the host in the inventory and removes the finalizer.
func (r *BareMetalInstanceReconciler) handleDeletion(ctx context.Context, bareMetalInstance *v1alpha1.BareMetalInstance) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Deleting BareMetalInstance")

	// Management cleanup
	if controllerutil.ContainsFinalizer(bareMetalInstance, BareMetalInstanceManagementFinalizer) {
		log.Info("Running management cleanup", "finalizer", BareMetalInstanceManagementFinalizer)

		bareMetalInstance.Status.Phase = v1alpha1.BareMetalInstancePhaseDeleting

		if bareMetalInstance.Spec.TemplateID != "" && bareMetalInstance.Spec.TemplateID != shared.OsacNoopTemplate {
			if r.ProvisioningProvider == nil {
				err := fmt.Errorf("provisioning provider not configured for template %q", bareMetalInstance.Spec.TemplateID)
				return ctrl.Result{}, err
			}

			result, done, err := r.reconcileDeprovisioning(ctx, bareMetalInstance)
			if err != nil {
				return result, err
			}
			if !done {
				return result, nil
			}
		}

		controllerutil.RemoveFinalizer(bareMetalInstance, BareMetalInstanceManagementFinalizer)
		if err := r.Update(ctx, bareMetalInstance); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Management cleanup completed")
	}

	// Inventory cleanup
	if !controllerutil.ContainsFinalizer(bareMetalInstance, BareMetalInstanceInventoryFinalizer) {
		log.Info("No inventory finalizer present, deletion complete")
		return ctrl.Result{}, nil
	}

	hostID := bareMetalInstance.Spec.ExternalHostID
	if hostID != "" {
		log.Info("Unassigning host from inventory", "InventoryHostID", bareMetalInstance.Spec.ExternalHostID)

		if !inventory.TryLock(hostID) {
			log.Info("Could not acquire lock for host", "InventoryHostID", hostID)
			return ctrl.Result{RequeueAfter: r.TryLockFailPollIntervalDuration}, nil
		}
		defer inventory.Unlock(hostID)

		// Collect non-persistent inventory labels to remove
		var labelsToRemove []string
		if bareMetalInstance.Spec.InventoryLabels != nil {
			labelsToRemove = make([]string, 0, len(bareMetalInstance.Spec.InventoryLabels))
			for key := range bareMetalInstance.Spec.InventoryLabels {
				labelsToRemove = append(labelsToRemove, key)
			}
		}
		if _, ok := bareMetalInstance.GetPoolID(); ok {
			labelsToRemove = append(labelsToRemove, shared.OsacBareMetalPoolIDLabel)
		}

		err := r.InventoryClient.UnassignHost(ctx, hostID, labelsToRemove)
		if err != nil {
			log.Error(err, "Failed to unassign host in inventory")
			return ctrl.Result{}, err
		}
	}

	if controllerutil.RemoveFinalizer(bareMetalInstance, BareMetalInstanceInventoryFinalizer) {
		if err := r.Update(ctx, bareMetalInstance); err != nil {
			log.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
	}

	log.Info("Successfully un-fulfilled BareMetalInstance")
	return ctrl.Result{}, nil
}

func (r *BareMetalInstanceReconciler) reconcileDeprovisioning(ctx context.Context, bareMetalInstance *v1alpha1.BareMetalInstance) (ctrl.Result, bool, error) {
	log := logf.FromContext(ctx)

	if bareMetalInstance.Status.Jobs == nil {
		bareMetalInstance.Status.Jobs = []opv1alpha1.JobStatus{}
	}

	latestDeprovisionJob := provisioning.FindLatestJobByType(bareMetalInstance.Status.Jobs, opv1alpha1.JobTypeDeprovision)

	if !provisioning.HasJobID(latestDeprovisionJob) {
		result, err := provisioning.TriggerDeprovisionJob(
			ctx, r.ProvisioningProvider, bareMetalInstance,
			&bareMetalInstance.Status.Jobs, provisioning.DefaultMaxJobHistory, r.ProvisionPollIntervalDuration,
		)
		if err != nil {
			log.Error(err, "Failed to trigger deprovision job")
			bareMetalInstance.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionFalse,
				v1alpha1.HostConditionReasonTemplateFailed,
				"Failed to trigger deprovision job",
			)
			return result, false, err
		}
		if err := r.Status().Update(ctx, bareMetalInstance); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("failed to flush status after deprovision trigger: %w", err)
		}
		if !result.IsZero() {
			bareMetalInstance.SetStatusCondition(
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
		ctx, r.ProvisioningProvider, bareMetalInstance,
		&bareMetalInstance.Status.Jobs, latestDeprovisionJob, r.ProvisionPollIntervalDuration,
	)
	if err != nil {
		return result, false, err
	}

	if done {
		if latestDeprovisionJob.State.IsSuccessful() {
			bareMetalInstance.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionTrue,
				"Succeeded",
				"Deprovision job completed successfully",
			)
		}
	} else {
		bareMetalInstance.SetStatusCondition(
			v1alpha1.HostConditionDeprovisionTemplateComplete,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"Deprovision job in progress",
		)
	}

	return result, done, nil
}
