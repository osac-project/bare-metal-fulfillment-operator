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

package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/osac-project/bare-metal-fulfillment-operator/internal/shared"
)

var (
	_ Client        = (*Metal3Client)(nil)
	_ NewClientFunc = NewClientFunc(NewMetal3Client)
)

const (
	metal3LabelPrefix = shared.OsacPrefix + "/"

	Metal3HostTypeLabel   = metal3LabelPrefix + "host-type"
	Metal3ManagedByLabel  = metal3LabelPrefix + "managed-by"
	Metal3InstanceIDLabel = metal3LabelPrefix + "instance-id"
	Metal3PoolIDLabel     = metal3LabelPrefix + "pool-id"
)

var acceptableProvisioningStates = map[metal3api.ProvisioningState]bool{
	"ready":                  true,
	"available":              true,
	"externally provisioned": true,
}

func init() {
	newClientFuncs["metal3"] = NewMetal3Client
}

type Metal3Client struct {
	client       client.Client
	namespace    string
	hostClass    string
	networkClass string
}

func NewMetal3Client(ctx context.Context, cfg *Config) (Client, error) {
	namespace, err := parseMetal3Namespace(cfg)
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err := metal3api.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add metal3 types to scheme: %w", err)
	}

	restConfig := ctrl.GetConfigOrDie()

	if err := validateBareMetalHostCRD(restConfig); err != nil {
		return nil, err
	}

	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	return &Metal3Client{
		client:       k8sClient,
		namespace:    namespace,
		hostClass:    cfg.HostClass,
		networkClass: cfg.NetworkClass,
	}, nil
}

func parseMetal3Namespace(cfg *Config) (string, error) {
	metal3Opts, ok := cfg.Options["metal3"]
	if !ok {
		return "", fmt.Errorf("metal3 options not found in config")
	}

	optsJSON, err := json.Marshal(metal3Opts)
	if err != nil {
		return "", fmt.Errorf("failed to marshal metal3 options: %w", err)
	}

	var opts struct {
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal(optsJSON, &opts); err != nil {
		return "", fmt.Errorf("failed to unmarshal metal3 options: %w", err)
	}

	if opts.Namespace == "" {
		return "", fmt.Errorf("metal3 namespace is required in config")
	}

	return opts.Namespace, nil
}

func validateBareMetalHostCRD(restConfig *rest.Config) error {
	dc, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}
	_, err = dc.ServerResourcesForGroupVersion("metal3.io/v1alpha1")
	if err != nil {
		return fmt.Errorf("metal3 backend configured but BareMetalHost CRD is not installed: %w", err)
	}
	return nil
}

func (m *Metal3Client) FindFreeHost(ctx context.Context, matchExpressions map[string]string) (*Host, error) {
	log := ctrllog.FromContext(ctx)
	log.Info("Finding free Metal3 host", "namespace", m.namespace)

	bmhList := &metal3api.BareMetalHostList{}
	if err := m.client.List(ctx, bmhList, client.InNamespace(m.namespace)); err != nil {
		return nil, fmt.Errorf("failed to list BareMetalHosts: %w", err)
	}

	matchHostType := matchExpressions["hostType"]
	matchManagedBy := matchExpressions["managedBy"]
	if matchManagedBy == "" {
		matchManagedBy = shared.OsacDefaultManagedByValue
	}

	var candidates []metal3api.BareMetalHost
	for _, bmh := range bmhList.Items {
		if bmh.Status.OperationalStatus != metal3api.OperationalStatusOK {
			continue
		}

		if !acceptableProvisioningStates[bmh.Status.Provisioning.State] {
			continue
		}

		labels := bmh.Labels
		if labels == nil {
			labels = map[string]string{}
		}

		if _, assigned := labels[Metal3InstanceIDLabel]; assigned {
			continue
		}

		if matchHostType != "" {
			if labels[Metal3HostTypeLabel] != matchHostType {
				continue
			}
		}

		hostManagedBy := labels[Metal3ManagedByLabel]
		if hostManagedBy == "" {
			hostManagedBy = shared.OsacDefaultManagedByValue
		}
		if hostManagedBy != matchManagedBy {
			continue
		}

		candidates = append(candidates, bmh)
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	bmh := &candidates[0]
	return bmhToHost(bmh, m.hostClass, m.networkClass), nil
}

func (m *Metal3Client) AssignHost(ctx context.Context, inventoryHostID string, bareMetalInstanceID string, labels map[string]string) (*Host, error) {
	if inventoryHostID == "" {
		return nil, fmt.Errorf("invalid input: inventoryHostID is empty")
	}
	if bareMetalInstanceID == "" {
		return nil, fmt.Errorf("invalid input: bareMetalInstanceID is empty")
	}

	namespace, name, err := ParseHostID(inventoryHostID)
	if err != nil {
		return nil, err
	}

	bmh := &metal3api.BareMetalHost{}
	if err := m.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, bmh); err != nil {
		return nil, fmt.Errorf("failed to get BareMetalHost %s: %w", inventoryHostID, err)
	}

	if bmh.Labels != nil {
		if currentID, ok := bmh.Labels[Metal3InstanceIDLabel]; ok && currentID != "" && currentID != bareMetalInstanceID {
			return nil, nil
		}
	}

	patchLabels := map[string]string{
		Metal3InstanceIDLabel: bareMetalInstanceID,
	}
	for key, value := range labels {
		patchLabels[metal3LabelPrefix+key] = value
	}

	patch := buildLabelPatch(patchLabels, nil)
	if err := m.client.Patch(ctx, bmh, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return nil, fmt.Errorf("failed to assign BareMetalHost %s: %w", inventoryHostID, err)
	}

	if err := m.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, bmh); err != nil {
		return nil, fmt.Errorf("failed to re-read BareMetalHost %s after patch: %w", inventoryHostID, err)
	}

	return bmhToHost(bmh, m.hostClass, m.networkClass), nil
}

func (m *Metal3Client) UnassignHost(ctx context.Context, inventoryHostID string, labels []string) error {
	namespace, name, err := ParseHostID(inventoryHostID)
	if err != nil {
		return err
	}

	bmh := &metal3api.BareMetalHost{}
	if err := m.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, bmh); err != nil {
		return fmt.Errorf("failed to get BareMetalHost %s: %w", inventoryHostID, err)
	}

	labelsToRemove := []string{Metal3InstanceIDLabel}
	for _, label := range labels {
		labelsToRemove = append(labelsToRemove, metal3LabelPrefix+label)
	}

	patch := buildLabelPatch(nil, labelsToRemove)
	if err := m.client.Patch(ctx, bmh, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("failed to unassign BareMetalHost %s: %w", inventoryHostID, err)
	}

	return nil
}

func ParseHostID(hostID string) (namespace, name string, err error) {
	parts := strings.SplitN(hostID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid host ID %q: expected namespace/name format", hostID)
	}
	return parts[0], parts[1], nil
}

func bmhToHost(bmh *metal3api.BareMetalHost, hostClass, networkClass string) *Host {
	labels := bmh.Labels
	if labels == nil {
		labels = map[string]string{}
	}

	managedBy := labels[Metal3ManagedByLabel]
	if managedBy == "" {
		managedBy = shared.OsacDefaultManagedByValue
	}

	return &Host{
		BareMetalPoolID:     labels[Metal3PoolIDLabel],
		BareMetalInstanceID: labels[Metal3InstanceIDLabel],
		InventoryHostID:     fmt.Sprintf("%s/%s", bmh.Namespace, bmh.Name),
		Name:                bmh.Name,
		HostType:            labels[Metal3HostTypeLabel],
		HostClass:           hostClass,
		NetworkClass:        networkClass,
		ProvisionState:      string(bmh.Status.Provisioning.State),
		ManagedBy:           managedBy,
	}
}

func buildLabelPatch(setLabels map[string]string, removeLabels []string) []byte {
	labelPatch := make(map[string]interface{})
	for k, v := range setLabels {
		labelPatch[k] = v
	}
	for _, k := range removeLabels {
		labelPatch[k] = nil
	}

	patchMap := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": labelPatch,
		},
	}

	patchBytes, _ := json.Marshal(patchMap)
	return patchBytes
}
