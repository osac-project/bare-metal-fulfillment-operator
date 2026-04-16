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
	"fmt"
	"time"

	"github.com/osac-project/bare-metal-fulfillment-operator/internal/shared"
)

const (
	// NoFreeHostsPollIntervalDuration is the default polling interval when no free hosts are available
	DefaultNoFreeHostsPollIntervalDuration = 30 * time.Second

	// TryLockFailPollIntervalDuration is the default polling interval when lock acquisition fails
	DefaultTryLockFailPollIntervalDuration = 1 * time.Second
)

var (
	// HostLeaseInventoryFinalizer is the finalizer added to HostLease resources for inventory cleanup
	HostLeaseInventoryFinalizer string = fmt.Sprintf("%s/inventory", shared.OsacPrefix)
)
