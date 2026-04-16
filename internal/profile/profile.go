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

// Package profile provides profile loading and matching functionality
package profile

import (
	"fmt"

	"sigs.k8s.io/yaml"
)

var (
	registeredProfiles = map[string]*Profile{}
)

// LoadProfiles registers profiles into the internal registry
func LoadProfiles(profiles []*Profile) error {
	for _, profile := range profiles {
		if profile.Name == "" {
			return fmt.Errorf("profile missing required 'name' field")
		}
		if _, exists := registeredProfiles[profile.Name]; exists {
			return fmt.Errorf("duplicate profile name: %s", profile.Name)
		}
		registeredProfiles[profile.Name] = profile
	}

	return nil
}

// Get returns a registered profile or nil if it doesn't exist
func Get(name string) *Profile {
	return registeredProfiles[name]
}

// Profile defines a configuration profile with workflows and host selection
type Profile struct {
	Name                       string            `yaml:"name"`
	HostSelector               map[string]string `yaml:"hostSelector"`
	ExpectedTemplateParameters []string          `yaml:"expectedTemplateParameters"`
	BareMetalPoolTemplate      string            `yaml:"bareMetalPoolTemplate,omitempty"`
	HostTemplate               string            `yaml:"hostTemplate,omitempty"`
}

func (p *Profile) ValidateParameters(templateParameters string) bool {
	if templateParameters == "" {
		return len(p.ExpectedTemplateParameters) == 0
	}

	// Parse the JSON string using yaml.Unmarshal (which can handle JSON)
	var params map[string]any
	if err := yaml.Unmarshal([]byte(templateParameters), &params); err != nil {
		return false // Invalid JSON/YAML
	}

	// Check if the number of keys matches exactly
	if len(params) != len(p.ExpectedTemplateParameters) {
		return false
	}

	// Convert p.ExpectedTemplateParameters slice to a set for O(1) lookup
	expectedKeys := make(map[string]struct{})
	for _, key := range p.ExpectedTemplateParameters {
		expectedKeys[key] = struct{}{}
	}

	// Check that all keys in the JSON are expected
	for key := range params {
		if _, ok := expectedKeys[key]; !ok {
			return false
		}
	}

	return true
}
