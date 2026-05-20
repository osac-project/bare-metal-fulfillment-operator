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
	"math/rand"
	"strings"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/baremetal/v1/nodes"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/gophercloud/utils/v2/openstack/clientconfig"

	"github.com/osac-project/bare-metal-fulfillment-operator/internal/shared"
)

var (
	_ Client        = (*OpenStackClient)(nil)
	_ NewClientFunc = NewClientFunc(NewOpenStackClient)
)

const (
	OSACPrefix = "osac_"

	// Label keys within osac_labels map
	PoolIDLabel    = "poolId"
	HostIDLabel    = "hostId"
	ManagedByLabel = "managedBy"
)

func init() {
	newClientFuncs["openstack"] = NewOpenStackClient
}

type OpenStackClient struct {
	client       *gophercloud.ServiceClient
	HostClass    string
	NetworkClass string
}

// NewOpenStackClient creates a new OpenStack inventory client
func NewOpenStackClient(ctx context.Context, cfg *Config) (Client, error) {
	opts := cfg.Options

	var cloud clientconfig.Cloud
	if openstackOpts, ok := opts["openstack"]; ok {
		openstackOptsJSON, err := json.Marshal(openstackOpts)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(openstackOptsJSON, &cloud); err != nil {
			return nil, err
		}
	}

	clientOpts := clientconfig.ClientOpts{
		Cloud:        cloud.Cloud,
		AuthType:     cloud.AuthType,
		AuthInfo:     cloud.AuthInfo,
		RegionName:   cloud.RegionName,
		EndpointType: cloud.EndpointType,
	}

	providerClient, err := clientconfig.AuthenticatedClient(ctx, &clientOpts)
	if err != nil {
		return nil, err
	}

	ironicClient, err := openstack.NewBareMetalV1(providerClient, gophercloud.EndpointOpts{})
	if err != nil {
		return nil, err
	}

	ironicClient.Microversion = "latest"

	return &OpenStackClient{
		client:       ironicClient,
		HostClass:    cfg.HostClass,
		NetworkClass: cfg.NetworkClass,
	}, nil
}

func (c *OpenStackClient) FindFreeHost(ctx context.Context, matchExpressions map[string]string) (*Host, error) {
	listOpts := nodes.ListOpts{
		Fields: []string{
			"uuid",
			"name",
			"resource_class",
			"provision_state",
			"extra",
		},
	}

	if hostType, ok := matchExpressions["hostType"]; ok {
		listOpts.ResourceClass = hostType
	}
	provisionState, ok := matchExpressions["provisionState"]
	if !ok || provisionState == "" {
		provisionState = shared.OsacDefaultProvisionStateValue
	}
	listOpts.ProvisionState = nodes.ProvisionState(provisionState)

	var foundHost *Host
	err := nodes.List(c.client, listOpts).EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		nodeList, err := nodes.ExtractNodes(page)
		if err != nil {
			return false, err
		}

		// shuffle to reduce chances of getting an unmarked but locked host
		nodes := make([]*nodes.Node, len(nodeList))
		for i := range nodeList {
			nodes[i] = &nodeList[i]
		}
		rand.Shuffle(len(nodes), func(i int, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})

		for _, node := range nodes {
			// Check if host is already assigned by looking for poolId or hostId labels
			poolID, _ := getNestedLabel(node, PoolIDLabel)
			hostID, _ := getNestedLabel(node, HostIDLabel)

			if poolID != "" || hostID != "" {
				continue
			}

			// Get managedBy label, defaulting to standard value if not set
			managedBy, ok := getNestedLabel(node, ManagedByLabel)
			if !ok || managedBy == "" {
				managedBy = shared.OsacDefaultManagedByValue
			}
			matchManagedBy, ok := matchExpressions["managedBy"]
			if !ok || matchManagedBy == "" {
				matchManagedBy = shared.OsacDefaultManagedByValue
			}
			if managedBy != matchManagedBy {
				continue
			}

			foundHost = &Host{
				BareMetalPoolID:     poolID,
				BareMetalPoolHostID: hostID,
				InventoryHostID:     node.UUID,
				Name:                node.Name,
				HostType:            node.ResourceClass,
				HostClass:           c.HostClass,
				NetworkClass:        c.NetworkClass,
				ProvisionState:      node.ProvisionState,
				ManagedBy:           managedBy,
			}
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return foundHost, nil
}

func (c *OpenStackClient) AssignHost(ctx context.Context, inventoryHostID string, poolID string, hostID string, labels map[string]string) (*Host, error) {
	node, err := nodes.Get(ctx, c.client, inventoryHostID).Extract()
	if err != nil {
		return nil, err
	}

	currentBareMetalPoolID, ok := getNestedLabel(node, PoolIDLabel)
	if ok && currentBareMetalPoolID != "" && currentBareMetalPoolID != poolID {
		return nil, nil
	}

	currentBareMetalPoolHostID, ok := getNestedLabel(node, HostIDLabel)
	if ok && currentBareMetalPoolHostID != "" && currentBareMetalPoolHostID != hostID {
		return nil, nil
	}

	// Ensure /extra/osac_labels exists before adding any labels
	if _, ok := node.Extra["osac_labels"]; !ok {
		initOpts := make(nodes.UpdateOpts, 0, 1)
		initOpts = append(initOpts, nodes.UpdateOperation{
			Op:    nodes.AddOp,
			Path:  "/extra/osac_labels",
			Value: map[string]interface{}{},
		})
		_, err = nodes.Update(ctx, c.client, inventoryHostID, initOpts).Extract()
		if err != nil {
			return nil, err
		}
	}

	// Add poolId, hostId, and user labels to osac_labels
	updateOpts := make(nodes.UpdateOpts, 0, 2+len(labels))
	updateOpts = append(updateOpts,
		nodes.UpdateOperation{
			Op:    nodes.AddOp,
			Path:  "/extra/osac_labels/" + escapeJSONPointerToken(PoolIDLabel),
			Value: poolID,
		},
		nodes.UpdateOperation{
			Op:    nodes.AddOp,
			Path:  "/extra/osac_labels/" + escapeJSONPointerToken(HostIDLabel),
			Value: hostID,
		},
	)

	// Add additional profile labels
	for labelKey, labelValue := range labels {
		updateOpts = append(updateOpts, nodes.UpdateOperation{
			Op:    nodes.AddOp,
			Path:  "/extra/osac_labels/" + escapeJSONPointerToken(labelKey),
			Value: labelValue,
		})
	}

	node, err = nodes.Update(ctx, c.client, inventoryHostID, updateOpts).Extract()
	if err != nil {
		return nil, err
	}

	managedBy, ok := getNestedLabel(node, ManagedByLabel)
	if !ok {
		managedBy = shared.OsacDefaultManagedByValue
	}

	return &Host{
		BareMetalPoolID:     poolID,
		BareMetalPoolHostID: hostID,
		InventoryHostID:     node.UUID,
		Name:                node.Name,
		HostType:            node.ResourceClass,
		HostClass:           c.HostClass,
		NetworkClass:        c.NetworkClass,
		ProvisionState:      node.ProvisionState,
		ManagedBy:           managedBy,
	}, nil
}

func (c *OpenStackClient) UnassignHost(ctx context.Context, inventoryHostID string, labels []string) error {
	// Get current node state to check what labels exist
	node, err := nodes.Get(ctx, c.client, inventoryHostID).Extract()
	if err != nil {
		return err
	}

	existing, _ := node.Extra["osac_labels"].(map[string]any)
	if existing == nil {
		existing = make(map[string]any)
	}

	// Build list of labels to remove: poolId, hostId, and user-provided labels
	// Note: managedBy is kept as a persistent label
	labelsToRemove := make([]string, 2, 2+len(labels))
	labelsToRemove[0] = PoolIDLabel
	labelsToRemove[1] = HostIDLabel
	labelsToRemove = append(labelsToRemove, labels...)

	updateOpts := make(nodes.UpdateOpts, 0, len(labelsToRemove))
	for _, label := range labelsToRemove {
		// Only remove if the label exists
		if _, ok := existing[label]; !ok {
			continue
		}
		updateOpts = append(updateOpts, nodes.UpdateOperation{
			Op:   nodes.RemoveOp,
			Path: "/extra/osac_labels/" + escapeJSONPointerToken(label),
		})
	}

	// If no labels to remove, nothing to do
	if len(updateOpts) == 0 {
		return nil
	}

	_, err = nodes.Update(ctx, c.client, inventoryHostID, updateOpts).Extract()
	return err
}

func escapeJSONPointerToken(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}

// getNestedLabel retrieves a label value from node.Extra["osac_labels"][labelKey]
// Returns the value as a string and a boolean indicating if it was found
func getNestedLabel(node *nodes.Node, labelKey string) (string, bool) {
	if labelsMap, ok := node.Extra["osac_labels"].(map[string]interface{}); ok {
		if value, ok := labelsMap[labelKey].(string); ok {
			return value, true
		}
	}
	return "", false
}
