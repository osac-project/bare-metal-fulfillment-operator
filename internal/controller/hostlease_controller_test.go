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
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/osac-project/bare-metal-fulfillment-operator/api/v1alpha1"
	"github.com/osac-project/bare-metal-fulfillment-operator/internal/inventory"
	"github.com/osac-project/bare-metal-fulfillment-operator/internal/management"
	"github.com/osac-project/bare-metal-fulfillment-operator/internal/shared"
	opv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac-operator/pkg/provisioning"
)

// mockInventoryClient implements inventory.Client for testing
type mockInventoryClient struct {
	findFreeHostFunc func(ctx context.Context, matchExpressions map[string]string) (*inventory.Host, error)
	assignHostFunc   func(ctx context.Context, inventoryHostID string, hostLeaseID string, labels map[string]string) (*inventory.Host, error)
	unassignHostFunc func(ctx context.Context, inventoryHostID string, labels []string) error
}

func (m *mockInventoryClient) FindFreeHost(ctx context.Context, matchExpressions map[string]string) (*inventory.Host, error) {
	if m.findFreeHostFunc != nil {
		return m.findFreeHostFunc(ctx, matchExpressions)
	}
	return nil, nil
}

func (m *mockInventoryClient) AssignHost(ctx context.Context, inventoryHostID string, hostLeaseID string, labels map[string]string) (*inventory.Host, error) {
	if m.assignHostFunc != nil {
		return m.assignHostFunc(ctx, inventoryHostID, hostLeaseID, labels)
	}
	return nil, nil
}

func (m *mockInventoryClient) UnassignHost(ctx context.Context, inventoryHostID string, labels []string) error {
	if m.unassignHostFunc != nil {
		return m.unassignHostFunc(ctx, inventoryHostID, labels)
	}
	return nil
}

// mockManagementClient implements management.Client for testing
type mockManagementClient struct {
	getPowerStateFunc func(ctx context.Context, hostID string) (*management.PowerStatus, error)
	setPowerStateFunc func(ctx context.Context, hostID string, target management.PowerState) error
}

func (m *mockManagementClient) GetPowerState(ctx context.Context, hostID string) (*management.PowerStatus, error) {
	if m.getPowerStateFunc != nil {
		return m.getPowerStateFunc(ctx, hostID)
	}
	return &management.PowerStatus{State: management.PowerOff}, nil
}

func (m *mockManagementClient) SetPowerState(ctx context.Context, hostID string, target management.PowerState) error {
	if m.setPowerStateFunc != nil {
		return m.setPowerStateFunc(ctx, hostID, target)
	}
	return nil
}

// mockProvisioningProvider implements provisioning.ProvisioningProvider for testing
type mockProvisioningProvider struct {
	triggerProvisionFunc     func(ctx context.Context, resource client.Object) (*provisioning.ProvisionResult, error)
	getProvisionStatusFunc   func(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error)
	triggerDeprovisionFunc   func(ctx context.Context, resource client.Object) (*provisioning.DeprovisionResult, error)
	getDeprovisionStatusFunc func(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error)
	nameFunc                 func() string
}

func (m *mockProvisioningProvider) TriggerProvision(ctx context.Context, resource client.Object) (*provisioning.ProvisionResult, error) {
	if m.triggerProvisionFunc != nil {
		return m.triggerProvisionFunc(ctx, resource)
	}
	return &provisioning.ProvisionResult{
		JobID:        "test-job-id",
		InitialState: opv1alpha1.JobStatePending,
		Message:      "Provision triggered",
	}, nil
}

func (m *mockProvisioningProvider) GetProvisionStatus(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error) {
	if m.getProvisionStatusFunc != nil {
		return m.getProvisionStatusFunc(ctx, resource, jobID)
	}
	return provisioning.ProvisionStatus{
		JobID:   jobID,
		State:   opv1alpha1.JobStateSucceeded,
		Message: "Provision completed",
	}, nil
}

func (m *mockProvisioningProvider) TriggerDeprovision(ctx context.Context, resource client.Object) (*provisioning.DeprovisionResult, error) {
	if m.triggerDeprovisionFunc != nil {
		return m.triggerDeprovisionFunc(ctx, resource)
	}
	return &provisioning.DeprovisionResult{
		Action:                 provisioning.DeprovisionTriggered,
		JobID:                  "test-deprovision-job-id",
		BlockDeletionOnFailure: false,
	}, nil
}

func (m *mockProvisioningProvider) GetDeprovisionStatus(ctx context.Context, resource client.Object, jobID string) (provisioning.ProvisionStatus, error) {
	if m.getDeprovisionStatusFunc != nil {
		return m.getDeprovisionStatusFunc(ctx, resource, jobID)
	}
	return provisioning.ProvisionStatus{
		JobID:   jobID,
		State:   opv1alpha1.JobStateSucceeded,
		Message: "Deprovision completed",
	}, nil
}

func (m *mockProvisioningProvider) Name() string {
	if m.nameFunc != nil {
		return m.nameFunc()
	}
	return "mock-provider"
}

var _ = Describe("HostLease Controller", func() {
	var (
		ctx              context.Context
		reconciler       *HostLeaseReconciler
		mockK8sClient    *mockClient
		mockInvClient    *mockInventoryClient
		mockMgmtClient   *mockManagementClient
		mockProvProvider *mockProvisioningProvider
		hostLease        *v1alpha1.HostLease

		namespace string
		hostType  string
		hostClass string
	)

	BeforeEach(func() {
		ctx = context.Background()
		mockK8sClient = &mockClient{Client: k8sClient}
		mockInvClient = &mockInventoryClient{}
		mockMgmtClient = &mockManagementClient{}
		mockProvProvider = nil

		namespace = "default"
		hostType = "fc430"
		hostClass = "external-mgmt"

		reconciler = NewHostLeaseReconciler(
			mockK8sClient,
			k8sClient.Scheme(),
			mockInvClient,
			mockMgmtClient,
			mockProvProvider,
			0,
			0,
			0,
			0,
		)
	})

	Describe("NewHostLeaseReconciler", func() {
		Context("when interval duration parameters are zero or negative", func() {
			BeforeEach(func() {
				reconciler = NewHostLeaseReconciler(
					mockK8sClient,
					k8sClient.Scheme(),
					mockInvClient,
					mockMgmtClient,
					mockProvProvider,
					-1*time.Second,
					0,
					-5*time.Second,
					0,
				)
			})

			It("should set them to the default values", func() {
				Expect(reconciler.NoFreeHostsPollIntervalDuration).To(Equal(DefaultNoFreeHostsPollIntervalDuration))
				Expect(reconciler.TryLockFailPollIntervalDuration).To(Equal(DefaultTryLockFailPollIntervalDuration))
				Expect(reconciler.ManagementRecheckIntervalDuration).To(Equal(DefaultManagementRecheckIntervalDuration))
				Expect(reconciler.ProvisionPollIntervalDuration).To(Equal(DefaultProvisionPollIntervalDuration))
			})
		})

		Context("when interval duration parameters are positive", func() {
			It("should use the provided values", func() {
				customReconciler := NewHostLeaseReconciler(
					mockK8sClient,
					k8sClient.Scheme(),
					mockInvClient,
					mockMgmtClient,
					mockProvProvider,
					45*time.Second,
					2*time.Second,
					15*time.Second,
					60*time.Second,
				)

				Expect(customReconciler.NoFreeHostsPollIntervalDuration).To(Equal(45 * time.Second))
				Expect(customReconciler.TryLockFailPollIntervalDuration).To(Equal(2 * time.Second))
				Expect(customReconciler.ManagementRecheckIntervalDuration).To(Equal(15 * time.Second))
				Expect(customReconciler.ProvisionPollIntervalDuration).To(Equal(60 * time.Second))
			})
		})
	})

	Describe("reconcileInventory", func() {
		BeforeEach(func() {
			hostLease = &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "reconcileInventory-hostLease",
					Namespace: namespace,
					UID:       "test-uid-123",
					Finalizers: []string{
						HostLeaseInventoryFinalizer,
					},
				},
				Spec: v1alpha1.HostLeaseSpec{
					HostType: hostType,
					Selector: v1alpha1.HostSelectorSpec{
						HostSelector: map[string]string{
							"managedBy":      shared.OsacDefaultManagedByValue,
							"provisionState": shared.OsacDefaultProvisionStateValue,
						},
					},
				},
			}
		})

		Context("when the finalizer is missing", func() {
			BeforeEach(func() {
				Expect(controllerutil.RemoveFinalizer(hostLease, HostLeaseInventoryFinalizer)).To(BeTrue())
			})

			It("should add the finalizer and requeue", func() {
				mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
					hl := obj.(*v1alpha1.HostLease)
					Expect(controllerutil.ContainsFinalizer(hl, HostLeaseInventoryFinalizer)).To(BeTrue())
					return nil
				}

				result, err := reconciler.reconcileInventory(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseAllocating))
			})
		})

		Context("when no free hosts are available", func() {
			BeforeEach(func() {
				mockInvClient.findFreeHostFunc = func(ctx context.Context, matchExpressions map[string]string) (*inventory.Host, error) {
					return nil, nil
				}
			})

			It("should set phase to Failed and requeue after poll interval", func() {
				result, err := reconciler.reconcileInventory(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(Equal(DefaultNoFreeHostsPollIntervalDuration))
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseFailed))
			})
		})

		Context("when a free host is found", func() {
			BeforeEach(func() {
				mockInvClient.findFreeHostFunc = func(ctx context.Context, matchExpressions map[string]string) (*inventory.Host, error) {
					Expect(matchExpressions["hostType"]).To(Equal(hostType))
					return &inventory.Host{
						InventoryHostID: "host-abc-123",
						HostClass:       hostClass,
						ManagedBy:       shared.OsacDefaultManagedByValue,
						ProvisionState:  shared.OsacDefaultProvisionStateValue,
					}, nil
				}
			})

			It("should update ExternalHostID and requeue", func() {
				updateCalled := false
				mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
					updateCalled = true
					hl := obj.(*v1alpha1.HostLease)
					Expect(hl.Spec.ExternalHostID).To(Equal("host-abc-123"))
					return nil
				}

				result, err := reconciler.reconcileInventory(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(updateCalled).To(BeTrue())
			})
		})

		Context("when assigning an ExternalHostID", func() {
			BeforeEach(func() {
				hostLease.Spec.ExternalHostID = "host-xyz-456"
				mockInvClient.assignHostFunc = func(ctx context.Context, inventoryHostID string, hostLeaseID string, labels map[string]string) (*inventory.Host, error) {
					Expect(inventoryHostID).To(Equal("host-xyz-456"))
					Expect(hostLeaseID).To(Equal("test-uid-123"))
					return &inventory.Host{
						InventoryHostID: inventoryHostID,
						HostClass:       hostClass,
					}, nil
				}
			})

			It("should assign the host and update HostClass", func() {
				mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
					hl := obj.(*v1alpha1.HostLease)
					Expect(hl.Spec.HostClass).To(Equal(hostClass))
					return nil
				}

				result, err := reconciler.reconcileInventory(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseProgressing))
			})
		})

		Context("when the host is already assigned to another HostLease", func() {
			BeforeEach(func() {
				hostLease.Spec.ExternalHostID = "host-taken-789"
				mockInvClient.assignHostFunc = func(ctx context.Context, inventoryHostID string, hostLeaseID string, labels map[string]string) (*inventory.Host, error) {
					return nil, nil
				}
			})

			It("should unset ExternalHostID and requeue", func() {
				mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
					hl := obj.(*v1alpha1.HostLease)
					Expect(hl.Spec.ExternalHostID).To(Equal(""))
					return nil
				}

				result, err := reconciler.reconcileInventory(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
			})
		})
	})

	Describe("reconcileManagement", func() {
		BeforeEach(func() {
			ctx = context.Background()
			hostLease = &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "reconcileManagement-hostLease",
					Namespace: namespace,
					UID:       "test-uid-123",
					Finalizers: []string{
						HostLeaseInventoryFinalizer,
						HostLeaseManagementFinalizer,
					},
				},
				Spec: v1alpha1.HostLeaseSpec{
					HostType: hostType,
					Selector: v1alpha1.HostSelectorSpec{
						HostSelector: map[string]string{
							"managedBy":      shared.OsacDefaultManagedByValue,
							"provisionState": shared.OsacDefaultProvisionStateValue,
						},
					},
				},
			}
		})

		Context("when the finalizer is missing", func() {
			BeforeEach(func() {
				Expect(controllerutil.RemoveFinalizer(hostLease, HostLeaseManagementFinalizer)).To(BeTrue())
			})

			It("should add the finalizer", func() {
				mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
					hl := obj.(*v1alpha1.HostLease)
					Expect(controllerutil.ContainsFinalizer(hl, HostLeaseManagementFinalizer)).To(BeTrue())
					return nil
				}

				result, err := reconciler.reconcileManagement(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseProgressing))
			})
		})

		Context("when PoweredOn is nil", func() {
			BeforeEach(func() {
				hostLease.Spec.PoweredOn = nil
			})

			It("should skip reconcilePower", func() {
				mockMgmtClient.getPowerStateFunc = func(ctx context.Context, hostID string) (*management.PowerStatus, error) {
					return &management.PowerStatus{State: management.PowerOff}, nil
				}

				setPowerStateCalled := false
				mockMgmtClient.setPowerStateFunc = func(ctx context.Context, hostID string, target management.PowerState) error {
					setPowerStateCalled = true
					return nil
				}

				result, err := reconciler.reconcileManagement(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(setPowerStateCalled).To(BeFalse())

				Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
				Expect(*hostLease.Status.PoweredOn).To(BeFalse())
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseReady))
				condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOff))
			})
		})

		Context("when PoweredOn is set to be false", func() {
			BeforeEach(func() {
				hostLease.Spec.PoweredOn = ptr.To(false)
			})

			It("should update status", func() {
				mockMgmtClient.getPowerStateFunc = func(ctx context.Context, hostID string) (*management.PowerStatus, error) {
					return &management.PowerStatus{State: management.PowerOff}, nil
				}

				setPowerStateCalled := false
				mockMgmtClient.setPowerStateFunc = func(ctx context.Context, hostID string, target management.PowerState) error {
					setPowerStateCalled = true
					return nil
				}

				result, err := reconciler.reconcileManagement(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(setPowerStateCalled).To(BeFalse())

				Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
				Expect(*hostLease.Status.PoweredOn).To(BeFalse())
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseReady))
				condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOff))
			})
		})

		Context("when power is not yet converged", func() {
			It("should requeue to be turned on", func() {
				hostLease.Spec.PoweredOn = ptr.To(true)

				mockMgmtClient.getPowerStateFunc = func(ctx context.Context, hostID string) (*management.PowerStatus, error) {
					return &management.PowerStatus{State: management.PowerOff}, nil
				}

				setPowerStateCalled := false
				mockMgmtClient.setPowerStateFunc = func(ctx context.Context, hostID string, target management.PowerState) error {
					setPowerStateCalled = true
					return nil
				}

				result, err := reconciler.reconcileManagement(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(Equal(DefaultManagementRecheckIntervalDuration))
				Expect(setPowerStateCalled).To(BeTrue())
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseProgressing))
			})

			It("should requeue to be turned off", func() {
				hostLease.Spec.PoweredOn = ptr.To(false)

				mockMgmtClient.getPowerStateFunc = func(ctx context.Context, hostID string) (*management.PowerStatus, error) {
					return &management.PowerStatus{State: management.PowerOn}, nil
				}

				setPowerStateCalled := false
				mockMgmtClient.setPowerStateFunc = func(ctx context.Context, hostID string, target management.PowerState) error {
					setPowerStateCalled = true
					return nil
				}

				result, err := reconciler.reconcileManagement(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(Equal(DefaultManagementRecheckIntervalDuration))
				Expect(setPowerStateCalled).To(BeTrue())
				Expect(hostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseProgressing))
			})
		})
	})

	Describe("reconcileProvisioning", func() {
		BeforeEach(func() {
			ctx = context.Background()
			mockProvProvider = &mockProvisioningProvider{}
			reconciler.ProvisioningProvider = mockProvProvider
			hostLease = &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "reconcileProvisioning-hostLease",
					Namespace: namespace,
					UID:       "test-uid-123",
					Finalizers: []string{
						HostLeaseInventoryFinalizer,
						HostLeaseManagementFinalizer,
					},
				},
				Spec: v1alpha1.HostLeaseSpec{
					HostType:       hostType,
					ExternalHostID: "host-123",
					HostClass:      hostClass,
					TemplateID:     "image-provision",
				},
			}
		})

		Context("when a successful provision job exists", func() {
			BeforeEach(func() {
				hostLease.Status.Jobs = []opv1alpha1.JobStatus{
					{
						JobID:     "123",
						Type:      opv1alpha1.JobTypeProvision,
						State:     opv1alpha1.JobStateSucceeded,
						Message:   "successful",
						Timestamp: metav1.Now(),
					},
				}
			})

			It("should not re-trigger provisioning", func() {
				triggerCalled := false
				mockProvProvider.triggerProvisionFunc = func(ctx context.Context, resource client.Object) (*provisioning.ProvisionResult, error) {
					triggerCalled = true
					return nil, nil
				}

				result, err := reconciler.reconcileProvisioning(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(triggerCalled).To(BeFalse())
			})
		})
	})

	Describe("syncHostLeaseStatus", func() {
		var log logr.Logger

		BeforeEach(func() {
			log = logr.Discard()
			hostLease = &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "syncHostLeaseStatus-hostLease",
					Namespace: namespace,
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "host-123",
					HostClass:      hostClass,
				},
			}
		})

		Context("when there is an error", func() {
			It("should set PowerSynced to False", func() {
				reconciler.syncHostLeaseStatus(hostLease, nil, errors.New("ironic connection failed"), log)

				condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonIronicAPIFailure))
				Expect(condition.Message).To(Equal("failed to sync power status"))
			})
		})

		Context("when node is on", func() {
			It("should set PowerSynced to True", func() {
				powerStatus := &management.PowerStatus{State: management.PowerOn}
				reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

				Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
				Expect(*hostLease.Status.PoweredOn).To(BeTrue())

				condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOn))
			})
		})

		Context("when node is off", func() {
			It("should set PowerSynced to True", func() {
				powerStatus := &management.PowerStatus{State: management.PowerOff}
				reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

				Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
				Expect(*hostLease.Status.PoweredOn).To(BeFalse())

				condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOff))
			})
		})

		Context("when power state does not match desired", func() {
			BeforeEach(func() {
				hostLease.Spec.PoweredOn = ptr.To(true)
			})

			It("should set PowerSynced to False", func() {
				powerStatus := &management.PowerStatus{State: management.PowerOff}
				reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

				Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
				Expect(*hostLease.Status.PoweredOn).To(BeFalse())
				condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonProgressing))
			})
		})

		Context("when node is transitioning", func() {
			It("should set PowerSynced to False", func() {
				powerStatus := &management.PowerStatus{State: management.PowerOff, IsTransitioning: true}
				reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

				Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
				Expect(*hostLease.Status.PoweredOn).To(BeFalse())
				condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonProgressing))
				Expect(condition.Message).To(Equal("node power state is transitioning"))
			})
		})

		Context("when powerStatus is nil and no error", func() {
			It("should not modify status", func() {
				reconciler.syncHostLeaseStatus(hostLease, nil, nil, log)

				Expect(hostLease.Status.PoweredOn).To(BeNil())
				Expect(hostLease.Status.Conditions).To(BeEmpty())
			})
		})
	})

	Describe("reconcilePower", func() {
		var log logr.Logger
		var powerStatus *management.PowerStatus

		BeforeEach(func() {
			log = logr.Discard()
			hostLease = &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "reconcilePower-hostLease",
					Namespace: namespace,
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "host-123",
				},
			}
		})

		Context("when its currently off and should be on", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOff}
				hostLease.Spec.PoweredOn = ptr.To(true)
			})

			It("should power on", func() {
				var calledTarget management.PowerState
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, target management.PowerState) error {
					calledTarget = target
					return nil
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).NotTo(HaveOccurred())
				Expect(calledTarget).To(Equal(management.PowerOn))
			})
		})

		Context("when its currently on and should be off", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOn}
				hostLease.Spec.PoweredOn = ptr.To(false)
			})

			It("should power off", func() {
				var calledTarget management.PowerState
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, target management.PowerState) error {
					calledTarget = target
					return nil
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).NotTo(HaveOccurred())
				Expect(calledTarget).To(Equal(management.PowerOff))
			})
		})

		Context("when power state already matches desired on", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOn}
				hostLease.Spec.PoweredOn = ptr.To(true)
			})

			It("should not call SetPowerState", func() {
				called := false
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
					called = true
					return nil
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).NotTo(HaveOccurred())
				Expect(called).To(BeFalse())
			})
		})

		Context("when power state already matches desired off", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOff}
				hostLease.Spec.PoweredOn = ptr.To(false)
			})

			It("should not call SetPowerState", func() {
				called := false
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
					called = true
					return nil
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).NotTo(HaveOccurred())
				Expect(called).To(BeFalse())
			})
		})

		Context("when node is transitioning", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOff, IsTransitioning: true}
				hostLease.Spec.PoweredOn = ptr.To(true)
			})

			It("should skip SetPowerState", func() {
				called := false
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
					called = true
					return nil
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).NotTo(HaveOccurred())
				Expect(called).To(BeFalse())
			})
		})

		Context("when SetPowerState returns ErrTransitioning", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOff}
				hostLease.Spec.PoweredOn = ptr.To(true)
			})

			It("should not return error", func() {
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
					return management.ErrTransitioning
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when setting the power on fails", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOff}
				hostLease.Spec.PoweredOn = ptr.To(true)
			})

			It("should return error", func() {
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
					return errors.New("ironic API error")
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ironic API error"))
			})
		})

		Context("when setting the power off fails", func() {
			BeforeEach(func() {
				powerStatus = &management.PowerStatus{State: management.PowerOn}
				hostLease.Spec.PoweredOn = ptr.To(false)
			})

			It("should return error", func() {
				mockMgmtClient.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
					return errors.New("ironic API error")
				}

				err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ironic API error"))
			})
		})
	})

	Describe("handleDeletion", func() {
		BeforeEach(func() {
			hostLease = &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "handleDeletion-hostLease",
					Namespace:         namespace,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "host-to-delete",
				},
			}
		})

		Context("when inventory finalizer is present", func() {
			BeforeEach(func() {
				controllerutil.AddFinalizer(hostLease, HostLeaseInventoryFinalizer)
			})

			It("should unassign the host and remove finalizer", func() {
				unassignCalled := false
				mockInvClient.unassignHostFunc = func(ctx context.Context, inventoryHostID string, labels []string) error {
					unassignCalled = true
					Expect(inventoryHostID).To(Equal("host-to-delete"))
					return nil
				}

				mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
					hl := obj.(*v1alpha1.HostLease)
					Expect(controllerutil.ContainsFinalizer(hl, HostLeaseInventoryFinalizer)).To(BeFalse())
					return nil
				}

				result, err := reconciler.handleDeletion(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
				Expect(unassignCalled).To(BeTrue())
			})

			Context("when ExternalHostID is empty", func() {
				BeforeEach(func() {
					hostLease.Spec.ExternalHostID = ""
				})

				It("should remove finalizer without unassigning", func() {
					unassignCalled := false
					mockInvClient.unassignHostFunc = func(ctx context.Context, inventoryHostID string, labels []string) error {
						unassignCalled = true
						return nil
					}

					mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
						return nil
					}

					result, err := reconciler.handleDeletion(ctx, hostLease)

					Expect(err).NotTo(HaveOccurred())
					Expect(result).To(Equal(ctrl.Result{}))
					Expect(unassignCalled).To(BeFalse())
				})
			})

			Context("when management finalizer is present", func() {
				BeforeEach(func() {
					controllerutil.AddFinalizer(hostLease, HostLeaseManagementFinalizer)
				})

				Context("when ProvisioningProvider is nil", func() {
					BeforeEach(func() {
						reconciler.ProvisioningProvider = nil
						hostLease.Spec.TemplateID = "os-provision"
					})

					It("should return an error", func() {
						result, err := reconciler.handleDeletion(ctx, hostLease)

						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(ContainSubstring("provisioning provider not configured"))
						Expect(result).To(Equal(ctrl.Result{}))
					})
				})

				Context("when TemplateID is empty", func() {
					BeforeEach(func() {
						mockProvProvider = &mockProvisioningProvider{}
						reconciler.ProvisioningProvider = mockProvProvider
						hostLease.Spec.TemplateID = ""
					})

					It("should skip deprovision and remove management finalizer", func() {
						mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
							hl := obj.(*v1alpha1.HostLease)
							Expect(controllerutil.ContainsFinalizer(hl, HostLeaseManagementFinalizer)).To(BeFalse())
							return nil
						}

						result, err := reconciler.handleDeletion(ctx, hostLease)

						Expect(err).NotTo(HaveOccurred())
						Expect(result).To(Equal(ctrl.Result{}))
					})
				})

				Context("when TemplateID is noop", func() {
					BeforeEach(func() {
						mockProvProvider = &mockProvisioningProvider{}
						reconciler.ProvisioningProvider = mockProvProvider
						hostLease.Spec.TemplateID = shared.OsacNoopTemplate
					})

					It("should skip deprovision and remove management finalizer", func() {
						mockK8sClient.updateFunc = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
							hl := obj.(*v1alpha1.HostLease)
							Expect(controllerutil.ContainsFinalizer(hl, HostLeaseManagementFinalizer)).To(BeFalse())
							return nil
						}

						result, err := reconciler.handleDeletion(ctx, hostLease)

						Expect(err).NotTo(HaveOccurred())
						Expect(result).To(Equal(ctrl.Result{}))
					})
				})

				It("should trigger deprovision and requeue", func() {
					mockProvProvider = &mockProvisioningProvider{}
					reconciler.ProvisioningProvider = mockProvProvider
					hostLease.Spec.TemplateID = "os-provision"

					mockK8sClient.statusUpdateFunc = func(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						return nil
					}

					result, err := reconciler.handleDeletion(ctx, hostLease)

					Expect(err).NotTo(HaveOccurred())
					Expect(result.RequeueAfter).To(Equal(DefaultProvisionPollIntervalDuration))
				})
			})
		})

		Context("when inventory finalizer is not present", func() {
			It("should return immediately", func() {
				result, err := reconciler.handleDeletion(ctx, hostLease)

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{}))
			})
		})
	})
})
