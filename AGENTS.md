# bare-metal-fulfillment-operator

Kubernetes operator for managing bare-metal host pools in the OSAC project. Defines BareMetalPool and BareMetalInstance CRDs. Integrates with OpenStack inventory systems and Ironic for power management. Defines Profiles which run Ansible playbooks for additional configuration on the CRDs. Includes Helm charts for deployment.

## Critical Rules

- **Always `make manifests generate`** after modifying CRD types in `api/v1alpha1/*_types.go`
- **Always `make helm-crds`** after regenerating CRDs (or run `make check-helm-crds` to verify sync)
- **Never edit** `config/crd/`, `zz_generated.deepcopy.go` — these are generated
- **Always `go mod tidy`** before committing
- Run `make lint test` before committing

## Dev Environment

```bash
make build                     # Build (runs tests first)
make test                      # Unit tests (excludes e2e)
make test-e2e                  # E2E tests with Kind cluster
make lint                      # golangci-lint
make lint-fix                  # Auto-fix lint issues
make fmt                       # go fmt
make vet                       # go vet

make manifests                 # Generate CRD manifests + RBAC
make generate                  # Generate DeepCopy methods
make helm-crds                 # Sync CRDs into Helm chart
make check-helm-crds           # Verify Helm CRD templates match config/crd/bases

make run                       # Run controller locally
make install                   # Install CRDs into cluster
make deploy IMG=<registry>/bare-metal-fulfillment-operator:tag
make undeploy

make image-build IMG=<registry>/bare-metal-fulfillment-operator:tag
make image-push IMG=<registry>/bare-metal-fulfillment-operator:tag

# Pre-commit hooks
pre-commit run --all-files
```

## Repository Structure

```text
bare-metal-fulfillment-operator/
├── api/v1alpha1/              # CRD type definitions (BareMetalPool, BareMetalInstance)
├── cmd/
│   └── main.go                # Operator entry point
├── internal/
│   ├── controller/            # Reconciliation logic (pool + instance controllers)
│   ├── helpers/               # Utility functions
│   ├── inventory/             # BareMetalInstance's host inventory abstraction (pluggable backend interface)
│   ├── management/            # BareMetalInstance's host management (power control)
│   ├── profile/               # Profile configuration handling
│   └── shared/                # Shared utilities
├── charts/
│   ├── operator/              # Operator Helm chart (deployment, RBAC, service)
│   └── operator-crds/         # CRD-only Helm chart
├── config/
│   ├── crd/                   # Generated CRD manifests (DO NOT EDIT)
│   ├── default/               # Kustomize default overlay
│   ├── manager/               # Manager deployment manifest
│   ├── rbac/                  # Generated RBAC rules
│   ├── samples/               # Example CRs
│   └── scorecard/             # Operator scorecard config
├── hack/
│   ├── sync-helm-crds.py      # Sync CRDs to Helm chart
│   └── sync-helm-operator.py  # Sync operator manifests to Helm chart
├── test/
│   ├── e2e/                   # End-to-end tests
│   └── utils/                 # Test utilities
├── Makefile                   # Build, test, lint, deploy, Helm targets
├── go.mod                     # Go 1.26, controller-runtime, gophercloud
└── .golangci.yml              # Linter configuration
```

## Resources Managed

- **BareMetalPool** — defines host sets (type + replica count) with optional profile; phases: Progressing, Ready, Failed, Deleting
- **BareMetalInstance** — individual bare-metal host with inventory allocation and power lifecycle; phases: Allocating, Progressing, Ready, Failed, Deleting

## Architecture

```text
BareMetalPool CR (spec.hostSets: [{hostType, replicas}])
  ↓ (reconcile)
BareMetalPool Controller
  ↓ (creates)
BareMetalInstance CRs (one per host)
  ↓ (reconcile)
BareMetalInstance Controller
  ↓ (allocates via)
Inventory Client (OpenStack)
  ↓ (manages power via)
Management Client (Ironic)
```

### Key Subsystems

| Package | Purpose |
|---------|---------|
| `internal/controller/` | Pool and instance reconciliation (lifecycle, finalizers, status) |
| `internal/inventory/` | Host allocation abstraction — Pluggable backend interface and implementations, host locking |
| `internal/management/` | Power control — OpenStack Ironic integration |
| `internal/profile/` | Profile configuration and parameter injection |
| `internal/shared/` | Shared utilities across controllers |

### Helm Charts

Two charts in `charts/`:
- **operator-crds** — CRD definitions only, for installing CRDs independently
- **operator** — operator deployment, RBAC, service account, metrics

CRDs must stay in sync: after `make manifests`, run `make helm-crds` (uses `hack/sync-helm-crds.py`). CI enforces sync via `make check-helm-crds`.

## CI

GitHub Actions (`.github/workflows/`):
- **build-image.yaml** — runs tests, builds + pushes container image and manifests
- **helm-lint.yaml** — verifies Helm CRD sync (`make check-helm-crds`) + Helm lint on PRs
- **pre-commit.yaml** — pre-commit hooks + golangci-lint on PRs
- **publish-charts.yaml** — packages and pushes Helm charts to GHCR on version tags

## Code Quality

- **golangci-lint** with dupl, errcheck, ginkgolinter, goconst, gocyclo, revive, staticcheck, and more (see `.golangci.yml`)
- **Pre-commit hooks**: trailing-whitespace, yamllint (strict, excludes `config/`), golangci-lint
- **Tests**: Ginkgo v2 + Gomega with envtest
