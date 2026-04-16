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

	OpenStackBareMetalPoolIDKey     = OSACPrefix + "poolId"
	OpenStackBareMetalPoolHostIDKey = OSACPrefix + "hostId"
	OpenStackLabelManagedByKey      = OSACPrefix + "managedBy"
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
			poolID, ok := node.Extra[OpenStackBareMetalPoolIDKey].(string)
			if !ok {
				poolID = ""
			}

			hostID, ok := node.Extra[OpenStackBareMetalPoolHostIDKey].(string)
			if !ok {
				hostID = ""
			}

			if poolID != "" || hostID != "" {
				continue
			}

			managedBy, ok := node.Extra[OpenStackLabelManagedByKey].(string)
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

	currentBareMetalPoolID, ok := node.Extra[OpenStackBareMetalPoolIDKey].(string)
	if ok && currentBareMetalPoolID != "" && currentBareMetalPoolID != poolID {
		return nil, nil
	}

	currentBareMetalPoolHostID, ok := node.Extra[OpenStackBareMetalPoolHostIDKey].(string)
	if ok && currentBareMetalPoolHostID != "" && currentBareMetalPoolHostID != hostID {
		return nil, nil
	}

	updateOpts := make(nodes.UpdateOpts, 0, 3+len(labels))
	updateOpts = append(updateOpts,
		nodes.UpdateOperation{
			Op:    nodes.AddOp,
			Path:  "/extra/" + OpenStackBareMetalPoolIDKey,
			Value: poolID,
		},
		nodes.UpdateOperation{
			Op:    nodes.AddOp,
			Path:  "/extra/" + OpenStackBareMetalPoolHostIDKey,
			Value: hostID,
		},
	)

	// Ensure /extra/osac_labels exists before adding individual label keys
	if len(labels) > 0 {
		// Check if osac_labels already exists in node.Extra
		if _, hasLabels := node.Extra["osac_labels"]; !hasLabels {
			updateOpts = append(updateOpts, nodes.UpdateOperation{
				Op:    nodes.AddOp,
				Path:  "/extra/osac_labels",
				Value: map[string]interface{}{},
			})
		}

		for labelKey, labelValue := range labels {
			updateOpts = append(updateOpts, nodes.UpdateOperation{
				Op:    nodes.AddOp,
				Path:  "/extra/osac_labels/" + escapeJSONPointerToken(labelKey),
				Value: labelValue,
			})
		}
	}

	node, err = nodes.Update(ctx, c.client, inventoryHostID, updateOpts).Extract()
	if err != nil {
		return nil, err
	}

	managedBy, ok := node.Extra[OpenStackLabelManagedByKey].(string)
	if !ok {
		managedBy = ""
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
	updateOpts := make(nodes.UpdateOpts, 0, 2+len(labels))
	updateOpts = append(updateOpts,
		nodes.UpdateOperation{
			Op:    nodes.ReplaceOp,
			Path:  "/extra/" + OpenStackBareMetalPoolIDKey,
			Value: "",
		},
		nodes.UpdateOperation{
			Op:    nodes.ReplaceOp,
			Path:  "/extra/" + OpenStackBareMetalPoolHostIDKey,
			Value: "",
		},
	)
	if len(labels) > 0 {
		node, err := nodes.Get(ctx, c.client, inventoryHostID).Extract()
		if err != nil {
			return err
		}

		existing, _ := node.Extra["osac_labels"].(map[string]any)
		if existing == nil {
			existing = make(map[string]any)
		}

		for _, label := range labels {
			if _, ok := existing[label]; !ok {
				continue
			}
			updateOpts = append(updateOpts, nodes.UpdateOperation{
				Op:   nodes.RemoveOp,
				Path: "/extra/osac_labels/" + escapeJSONPointerToken(label),
			})
		}
	}

	_, err := nodes.Update(ctx, c.client, inventoryHostID, updateOpts).Extract()
	return err
}

func escapeJSONPointerToken(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}
