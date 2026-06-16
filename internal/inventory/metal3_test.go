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
	"testing"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace = "test-bmaas"
	testHostClass = "metal3"
	testNetClass  = "metal3"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = metal3api.AddToScheme(s)
	return s
}

func newMetal3ClientForTest(objects ...client.Object) *Metal3Client {
	scheme := newTestScheme()
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&metal3api.BareMetalHost{}).
		Build()
	return &Metal3Client{
		client:       fakeClient,
		namespace:    testNamespace,
		hostClass:    testHostClass,
		networkClass: testNetClass,
	}
}

func newBMH(name string, labels map[string]string, opStatus metal3api.OperationalStatus, provState metal3api.ProvisioningState) *metal3api.BareMetalHost {
	if labels == nil {
		labels = map[string]string{}
	}
	return &metal3api.BareMetalHost{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    labels,
		},
		Status: metal3api.BareMetalHostStatus{
			OperationalStatus: opStatus,
			Provisioning: metal3api.ProvisionStatus{
				State: provState,
			},
		},
	}
}

// --- ParseHostID ---

func TestParseHostID(t *testing.T) {
	tests := []struct {
		name      string
		hostID    string
		wantNS    string
		wantName  string
		wantError bool
	}{
		{
			name:     "valid namespace/name",
			hostID:   "my-namespace/my-host",
			wantNS:   "my-namespace",
			wantName: "my-host",
		},
		{
			name:      "missing namespace",
			hostID:    "/my-host",
			wantError: true,
		},
		{
			name:      "missing name",
			hostID:    "my-namespace/",
			wantError: true,
		},
		{
			name:      "no separator",
			hostID:    "just-a-name",
			wantError: true,
		},
		{
			name:      "empty string",
			hostID:    "",
			wantError: true,
		},
		{
			name:     "name with extra slashes",
			hostID:   "ns/name/extra",
			wantNS:   "ns",
			wantName: "name/extra",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, name, err := ParseHostID(tt.hostID)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ns != tt.wantNS {
				t.Errorf("namespace = %q, want %q", ns, tt.wantNS)
			}
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
		})
	}
}

// --- FindFreeHost ---

func TestFindFreeHost(t *testing.T) {
	ctx := context.Background()

	t.Run("returns matching unassigned host", func(t *testing.T) {
		bmh := newBMH("host-1", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected a host, got nil")
		}
		if host.InventoryHostID != testNamespace+"/host-1" {
			t.Errorf("InventoryHostID = %q, want %q", host.InventoryHostID, testNamespace+"/host-1")
		}
		if host.Name != "host-1" {
			t.Errorf("Name = %q, want %q", host.Name, "host-1")
		}
		if host.HostType != "gpu-node" {
			t.Errorf("HostType = %q, want %q", host.HostType, "gpu-node")
		}
		if host.HostClass != testHostClass {
			t.Errorf("HostClass = %q, want %q", host.HostClass, testHostClass)
		}
		if host.NetworkClass != testNetClass {
			t.Errorf("NetworkClass = %q, want %q", host.NetworkClass, testNetClass)
		}
		if host.ProvisionState != "available" {
			t.Errorf("ProvisionState = %q, want %q", host.ProvisionState, "available")
		}
		if host.ManagedBy != "baremetal" {
			t.Errorf("ManagedBy = %q, want %q", host.ManagedBy, "baremetal")
		}
	})

	t.Run("excludes hosts with instance-id label", func(t *testing.T) {
		assigned := newBMH("host-assigned", map[string]string{
			Metal3HostTypeLabel:   "gpu-node",
			Metal3InstanceIDLabel: "some-instance",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(assigned)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host != nil {
			t.Errorf("expected nil (host is assigned), got %+v", host)
		}
	})

	t.Run("excludes hosts with non-ok operational status", func(t *testing.T) {
		bmh := newBMH("host-error", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusError, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host != nil {
			t.Errorf("expected nil (host has error status), got %+v", host)
		}
	})

	t.Run("excludes hosts with unacceptable provisioning state", func(t *testing.T) {
		bmh := newBMH("host-provisioning", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusOK, "provisioning")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host != nil {
			t.Errorf("expected nil (host is provisioning), got %+v", host)
		}
	})

	t.Run("accepts ready provisioning state", func(t *testing.T) {
		bmh := newBMH("host-ready", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusOK, "ready")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected a host with ready state, got nil")
		}
	})

	t.Run("accepts externally provisioned state", func(t *testing.T) {
		bmh := newBMH("host-ext", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusOK, "externally provisioned")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected a host with externally provisioned state, got nil")
		}
	})

	t.Run("filters by host type label", func(t *testing.T) {
		gpuHost := newBMH("host-gpu", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusOK, "available")
		cpuHost := newBMH("host-cpu", map[string]string{
			Metal3HostTypeLabel: "cpu-node",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(gpuHost, cpuHost)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected gpu host, got nil")
		}
		if host.HostType != "gpu-node" {
			t.Errorf("HostType = %q, want %q", host.HostType, "gpu-node")
		}
	})

	t.Run("filters by managed-by label with default", func(t *testing.T) {
		bmh := newBMH("host-default-managed", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected host (both default to baremetal), got nil")
		}
	})

	t.Run("filters by managed-by label mismatch", func(t *testing.T) {
		bmh := newBMH("host-agent", map[string]string{
			Metal3HostTypeLabel:  "gpu-node",
			Metal3ManagedByLabel: "agent",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host != nil {
			t.Errorf("expected nil (managedBy mismatch), got %+v", host)
		}
	})

	t.Run("filters by explicit managed-by match expression", func(t *testing.T) {
		bmh := newBMH("host-agent", map[string]string{
			Metal3HostTypeLabel:  "gpu-node",
			Metal3ManagedByLabel: "agent",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{
			"hostType":  "gpu-node",
			"managedBy": "agent",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected host (managedBy matches), got nil")
		}
	})

	t.Run("returns nil when no matching hosts exist", func(t *testing.T) {
		m := newMetal3ClientForTest()
		host, err := m.FindFreeHost(ctx, map[string]string{"hostType": "gpu-node"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host != nil {
			t.Errorf("expected nil (no hosts), got %+v", host)
		}
	})

	t.Run("matches hosts without hostType filter", func(t *testing.T) {
		bmh := newBMH("host-any", map[string]string{},
			metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.FindFreeHost(ctx, map[string]string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected host (no hostType filter), got nil")
		}
	})
}

// --- AssignHost ---

func TestAssignHost(t *testing.T) {
	ctx := context.Background()

	t.Run("assigns host with instance-id and additional labels", func(t *testing.T) {
		bmh := newBMH("host-1", map[string]string{
			Metal3HostTypeLabel: "gpu-node",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.AssignHost(ctx, testNamespace+"/host-1", "instance-123", map[string]string{
			"profileName": "myProfile",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected assigned host, got nil")
		}
		if host.BareMetalInstanceID != "instance-123" {
			t.Errorf("BareMetalInstanceID = %q, want %q", host.BareMetalInstanceID, "instance-123")
		}

		// Verify labels were set on the BMH
		updatedBMH := &metal3api.BareMetalHost{}
		if err := m.client.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "host-1"}, updatedBMH); err != nil {
			t.Fatalf("failed to get updated BMH: %v", err)
		}
		if updatedBMH.Labels[Metal3InstanceIDLabel] != "instance-123" {
			t.Errorf("instance-id label = %q, want %q", updatedBMH.Labels[Metal3InstanceIDLabel], "instance-123")
		}
		if updatedBMH.Labels[metal3LabelPrefix+"profileName"] != "myProfile" {
			t.Errorf("profileName label = %q, want %q", updatedBMH.Labels[metal3LabelPrefix+"profileName"], "myProfile")
		}
	})

	t.Run("returns nil if host is assigned to a different instance", func(t *testing.T) {
		bmh := newBMH("host-taken", map[string]string{
			Metal3HostTypeLabel:   "gpu-node",
			Metal3InstanceIDLabel: "other-instance",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.AssignHost(ctx, testNamespace+"/host-taken", "my-instance", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host != nil {
			t.Errorf("expected nil (host taken by other), got %+v", host)
		}
	})

	t.Run("succeeds if host is already assigned to the same instance", func(t *testing.T) {
		bmh := newBMH("host-mine", map[string]string{
			Metal3HostTypeLabel:   "gpu-node",
			Metal3InstanceIDLabel: "my-instance",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		host, err := m.AssignHost(ctx, testNamespace+"/host-mine", "my-instance", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected host (idempotent assign), got nil")
		}
	})

	t.Run("returns error for empty inventoryHostID", func(t *testing.T) {
		m := newMetal3ClientForTest()
		_, err := m.AssignHost(ctx, "", "instance-123", nil)
		if err == nil {
			t.Fatal("expected error for empty hostID, got nil")
		}
	})

	t.Run("returns error for empty bareMetalInstanceID", func(t *testing.T) {
		m := newMetal3ClientForTest()
		_, err := m.AssignHost(ctx, testNamespace+"/host-1", "", nil)
		if err == nil {
			t.Fatal("expected error for empty instanceID, got nil")
		}
	})

	t.Run("returns error for invalid host ID format", func(t *testing.T) {
		m := newMetal3ClientForTest()
		_, err := m.AssignHost(ctx, "no-slash", "instance-123", nil)
		if err == nil {
			t.Fatal("expected error for invalid hostID, got nil")
		}
	})
}

// --- UnassignHost ---

func TestUnassignHost(t *testing.T) {
	ctx := context.Background()

	t.Run("removes instance-id and specified labels", func(t *testing.T) {
		bmh := newBMH("host-1", map[string]string{
			Metal3HostTypeLabel:               "gpu-node",
			Metal3InstanceIDLabel:             "instance-123",
			metal3LabelPrefix + "profileName": "myProfile",
			metal3LabelPrefix + "managedBy":   "agent",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		err := m.UnassignHost(ctx, testNamespace+"/host-1", []string{"profileName"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updatedBMH := &metal3api.BareMetalHost{}
		if err := m.client.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "host-1"}, updatedBMH); err != nil {
			t.Fatalf("failed to get updated BMH: %v", err)
		}
		if _, ok := updatedBMH.Labels[Metal3InstanceIDLabel]; ok {
			t.Error("instance-id label should have been removed")
		}
		if _, ok := updatedBMH.Labels[metal3LabelPrefix+"profileName"]; ok {
			t.Error("profileName label should have been removed")
		}
		// managed-by should still be present (not in remove list)
		if updatedBMH.Labels[metal3LabelPrefix+"managedBy"] != "agent" {
			t.Error("managedBy label should not have been removed")
		}
	})

	t.Run("handles no additional labels to remove", func(t *testing.T) {
		bmh := newBMH("host-2", map[string]string{
			Metal3InstanceIDLabel: "instance-456",
		}, metal3api.OperationalStatusOK, "available")

		m := newMetal3ClientForTest(bmh)
		err := m.UnassignHost(ctx, testNamespace+"/host-2", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updatedBMH := &metal3api.BareMetalHost{}
		if err := m.client.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "host-2"}, updatedBMH); err != nil {
			t.Fatalf("failed to get updated BMH: %v", err)
		}
		if _, ok := updatedBMH.Labels[Metal3InstanceIDLabel]; ok {
			t.Error("instance-id label should have been removed")
		}
	})

	t.Run("returns error for invalid host ID", func(t *testing.T) {
		m := newMetal3ClientForTest()
		err := m.UnassignHost(ctx, "invalid-id", nil)
		if err == nil {
			t.Fatal("expected error for invalid hostID, got nil")
		}
	})
}

// --- parseMetal3Namespace ---

func TestParseMetal3Namespace(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		want      string
		wantError bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				Options: map[string]any{
					"metal3": map[string]any{
						"namespace": "openshift-machine-api",
					},
				},
			},
			want: "openshift-machine-api",
		},
		{
			name: "missing metal3 key",
			cfg: &Config{
				Options: map[string]any{},
			},
			wantError: true,
		},
		{
			name: "empty namespace",
			cfg: &Config{
				Options: map[string]any{
					"metal3": map[string]any{
						"namespace": "",
					},
				},
			},
			wantError: true,
		},
		{
			name: "missing namespace key",
			cfg: &Config{
				Options: map[string]any{
					"metal3": map[string]any{},
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMetal3Namespace(tt.cfg)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("namespace = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- init registration ---

func TestMetal3BackendRegistration(t *testing.T) {
	t.Run("metal3 backend is registered in newClientFuncs", func(t *testing.T) {
		if _, ok := newClientFuncs["metal3"]; !ok {
			t.Fatal("metal3 backend not registered in newClientFuncs")
		}
	})
}
