# Project 12: Multi-Tenant Chat Workspaces
# TIER 3 -- Expert Platform Engineering

## Goal
Build a SaaS-style multi-tenant platform where each tenant gets isolated
resources, networking, and data. Implement namespace-per-tenant with
automated onboarding and resource governance.

---

## Architecture

```
  [API Gateway]
       |
  tenant-id header / subdomain
       |
  [Tenant Router Middleware]
       |
  +----+----+----+
  |         |         |
[tenant-a] [tenant-b] [tenant-c]
namespace  namespace  namespace
  |         |         |
[own DB]  [own DB]  [own DB]
[own quotas] [own quotas] [own quotas]
[own network] [own network] [own network]
```

---

## Directory Structure

```
12-multi-tenancy/
├── tenant-operator/
│   ├── main.go                     # Custom K8s operator
│   ├── api/v1/
│   │   └── tenant_types.go        # Tenant CRD
│   ├── controllers/
│   │   └── tenant_controller.go   # Reconciliation logic
│   └── config/
│       ├── crd/
│       │   └── tenant-crd.yaml
│       └── samples/
│           └── tenant-acme.yaml
├── templates/
│   ├── namespace.yaml.tmpl
│   ├── resource-quota.yaml.tmpl
│   ├── limit-range.yaml.tmpl
│   ├── network-policy.yaml.tmpl
│   ├── service-accounts.yaml.tmpl
│   └── database.yaml.tmpl
├── gateway/
│   ├── tenant-routing.yaml         # Kong/Emissary tenant routing
│   └── middleware/
│       └── tenant_resolver.go     # Extract tenant from request
├── services/
│   ├── tenant-aware-middleware/
│   │   ├── go/tenant.go
│   │   ├── typescript/tenant.ts
│   │   └── python/tenant.py
│   └── tenant-management-api/
│       ├── main.go
│       ├── handlers/
│       │   ├── create_tenant.go
│       │   ├── update_tenant.go
│       │   └── delete_tenant.go
│       └── models/
│           └── tenant.go
├── vcluster/                       # Stronger isolation option
│   ├── vcluster-values.yaml
│   └── tenant-vcluster.yaml
├── k8s/
│   └── platform-namespace.yaml
└── testing/
    ├── isolation-test.sh
    └── noisy-neighbor-test.sh
```

---

## Step-by-Step Implementation

### Phase 1: Tenant CRD + Operator (Day 1-5)

```yaml
# tenant-operator/config/crd/tenant-crd.yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: tenants.platform.example.com
spec:
  group: platform.example.com
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              required: ["name", "plan"]
              properties:
                name:
                  type: string
                plan:
                  type: string
                  enum: ["free", "starter", "business", "enterprise"]
                owner:
                  type: string
                customDomain:
                  type: string
            status:
              type: object
              properties:
                phase:
                  type: string
                namespace:
                  type: string
                databaseReady:
                  type: boolean
                message:
                  type: string
      subresources:
        status: {}
      additionalPrinterColumns:
        - name: Plan
          type: string
          jsonPath: .spec.plan
        - name: Phase
          type: string
          jsonPath: .status.phase
        - name: Namespace
          type: string
          jsonPath: .status.namespace
  scope: Cluster
  names:
    plural: tenants
    singular: tenant
    kind: Tenant
```

```yaml
# tenant-operator/config/samples/tenant-acme.yaml
apiVersion: platform.example.com/v1
kind: Tenant
metadata:
  name: acme-corp
spec:
  name: "ACME Corporation"
  plan: business
  owner: admin@acme.com
  customDomain: acme.example.com
```

```go
// tenant-operator/controllers/tenant_controller.go
package controllers

import (
    "context"
    "fmt"

    corev1 "k8s.io/api/core/v1"
    networkingv1 "k8s.io/api/networking/v1"
    "k8s.io/apimachinery/pkg/api/resource"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/controller-runtime/pkg/client"

    platformv1 "tenant-operator/api/v1"
)

type TenantReconciler struct {
    client.Client
}

func (r *TenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    var tenant platformv1.Tenant
    if err := r.Get(ctx, req.NamespacedName, &tenant); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    ns := fmt.Sprintf("tenant-%s", tenant.Name)

    // 1. Create namespace
    if err := r.ensureNamespace(ctx, ns, &tenant); err != nil {
        return ctrl.Result{}, err
    }

    // 2. Apply ResourceQuota based on plan
    if err := r.ensureResourceQuota(ctx, ns, tenant.Spec.Plan); err != nil {
        return ctrl.Result{}, err
    }

    // 3. Apply LimitRange
    if err := r.ensureLimitRange(ctx, ns); err != nil {
        return ctrl.Result{}, err
    }

    // 4. Apply NetworkPolicies (isolate from other tenants)
    if err := r.ensureNetworkPolicies(ctx, ns); err != nil {
        return ctrl.Result{}, err
    }

    // 5. Create tenant database
    if err := r.ensureDatabase(ctx, ns, &tenant); err != nil {
        return ctrl.Result{}, err
    }

    // 6. Deploy tenant services
    if err := r.deployServices(ctx, ns, &tenant); err != nil {
        return ctrl.Result{}, err
    }

    // 7. Update status
    tenant.Status.Phase = "Ready"
    tenant.Status.Namespace = ns
    tenant.Status.DatabaseReady = true
    r.Status().Update(ctx, &tenant)

    return ctrl.Result{}, nil
}

// Plan-based resource quotas
var planQuotas = map[string]corev1.ResourceList{
    "free": {
        corev1.ResourceRequestsCPU:    resource.MustParse("500m"),
        corev1.ResourceRequestsMemory: resource.MustParse("512Mi"),
        corev1.ResourceLimitsCPU:      resource.MustParse("1"),
        corev1.ResourceLimitsMemory:   resource.MustParse("1Gi"),
        corev1.ResourcePods:           resource.MustParse("10"),
    },
    "starter": {
        corev1.ResourceRequestsCPU:    resource.MustParse("2"),
        corev1.ResourceRequestsMemory: resource.MustParse("2Gi"),
        corev1.ResourceLimitsCPU:      resource.MustParse("4"),
        corev1.ResourceLimitsMemory:   resource.MustParse("4Gi"),
        corev1.ResourcePods:           resource.MustParse("20"),
    },
    "business": {
        corev1.ResourceRequestsCPU:    resource.MustParse("4"),
        corev1.ResourceRequestsMemory: resource.MustParse("8Gi"),
        corev1.ResourceLimitsCPU:      resource.MustParse("8"),
        corev1.ResourceLimitsMemory:   resource.MustParse("16Gi"),
        corev1.ResourcePods:           resource.MustParse("50"),
    },
    "enterprise": {
        corev1.ResourceRequestsCPU:    resource.MustParse("16"),
        corev1.ResourceRequestsMemory: resource.MustParse("32Gi"),
        corev1.ResourceLimitsCPU:      resource.MustParse("32"),
        corev1.ResourceLimitsMemory:   resource.MustParse("64Gi"),
        corev1.ResourcePods:           resource.MustParse("200"),
    },
}

func (r *TenantReconciler) ensureNetworkPolicies(ctx context.Context, ns string) error {
    // Default deny all ingress from other tenant namespaces
    np := &networkingv1.NetworkPolicy{
        ObjectMeta: metav1.ObjectMeta{Name: "isolate-tenant", Namespace: ns},
        Spec: networkingv1.NetworkPolicySpec{
            PodSelector: metav1.LabelSelector{},
            PolicyTypes: []networkingv1.PolicyType{
                networkingv1.PolicyTypeIngress,
                networkingv1.PolicyTypeEgress,
            },
            Ingress: []networkingv1.NetworkPolicyIngressRule{
                {
                    From: []networkingv1.NetworkPolicyPeer{
                        {
                            // Only allow traffic from same namespace
                            PodSelector: &metav1.LabelSelector{},
                        },
                        {
                            // Allow from ingress controller
                            NamespaceSelector: &metav1.LabelSelector{
                                MatchLabels: map[string]string{
                                    "app": "ingress-controller",
                                },
                            },
                        },
                    },
                },
            },
            Egress: []networkingv1.NetworkPolicyEgressRule{
                {
                    // Allow DNS
                    To: []networkingv1.NetworkPolicyPeer{},
                    Ports: []networkingv1.NetworkPolicyPort{
                        {Port: &intstr.IntOrString{IntVal: 53}, Protocol: &udp},
                    },
                },
                {
                    // Allow egress within same namespace only
                    To: []networkingv1.NetworkPolicyPeer{
                        {PodSelector: &metav1.LabelSelector{}},
                    },
                },
            },
        },
    }
    return r.Create(ctx, np)
}
```

### Phase 2: Tenant-Aware Middleware (Day 6-7)

```go
// services/tenant-aware-middleware/go/tenant.go
package middleware

import (
    "context"
    "net/http"
    "strings"
)

type contextKey string

const TenantIDKey contextKey = "tenant_id"

func TenantMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        tenantID := extractTenantID(r)
        if tenantID == "" {
            http.Error(w, "tenant not identified", http.StatusBadRequest)
            return
        }

        ctx := context.WithValue(r.Context(), TenantIDKey, tenantID)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}

func extractTenantID(r *http.Request) string {
    // Strategy 1: Header-based
    if tid := r.Header.Get("X-Tenant-ID"); tid != "" {
        return tid
    }

    // Strategy 2: Subdomain-based (acme.example.com -> acme)
    host := r.Host
    parts := strings.Split(host, ".")
    if len(parts) >= 3 {
        return parts[0]
    }

    // Strategy 3: JWT claim
    // Extract from authenticated token's "tenant_id" claim

    return ""
}

func GetTenantID(ctx context.Context) string {
    if v, ok := ctx.Value(TenantIDKey).(string); ok {
        return v
    }
    return ""
}
```

**Tenant-scoped database connections:**
```go
// Each tenant gets its own database connection string
type TenantDBPool struct {
    pools map[string]*sql.DB
    mu    sync.RWMutex
}

func (p *TenantDBPool) GetDB(tenantID string) (*sql.DB, error) {
    p.mu.RLock()
    if db, ok := p.pools[tenantID]; ok {
        p.mu.RUnlock()
        return db, nil
    }
    p.mu.RUnlock()

    // Lazy init: create connection for new tenant
    dsn := fmt.Sprintf("postgres://user:pass@postgres.tenant-%s:5432/appdb", tenantID)
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        return nil, err
    }

    p.mu.Lock()
    p.pools[tenantID] = db
    p.mu.Unlock()

    return db, nil
}
```

### Phase 3: Gateway Tenant Routing (Day 8-9)

```yaml
# gateway/tenant-routing.yaml
# Kong: route based on subdomain or header
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: tenant-routing
  annotations:
    konghq.com/plugins: tenant-resolver
spec:
  ingressClassName: kong
  rules:
    # Subdomain-based: acme.example.com
    - host: "*.example.com"
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: tenant-router
                port:
                  number: 8080
```

### Phase 4: vcluster for Stronger Isolation (Day 10-12)

```bash
# Install vcluster CLI
curl -L -o vcluster "https://github.com/loft-sh/vcluster/releases/latest/download/vcluster-linux-amd64"
chmod +x vcluster && sudo mv vcluster /usr/local/bin/

# Create a virtual cluster for a tenant
vcluster create tenant-acme -n tenant-acme --connect=false
```

```yaml
# vcluster/tenant-vcluster.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: tenant-acme-vcluster
---
# Helm-based vcluster creation
# Each enterprise tenant gets a full virtual K8s cluster
# They see their own namespaces, can create CRDs, etc.
# But it all runs within the host cluster's namespace

# vcluster gives:
# - Full cluster-admin within the virtual cluster
# - Complete namespace isolation
# - Own set of CRDs
# - Resource limits enforced by host namespace quotas
```

---

## Isolation Models

| Model | Isolation | Cost | Complexity | Best For |
|-------|-----------|------|------------|----------|
| **Schema-per-tenant** | Low | Low | Low | Free tier, many tenants |
| **Database-per-tenant** | Medium | Medium | Medium | Starter/Business tiers |
| **Namespace-per-tenant** | High | Medium | Medium | Business tier |
| **vcluster-per-tenant** | Very High | High | High | Enterprise tier |
| **Cluster-per-tenant** | Maximum | Very High | Very High | Regulated industries |

---

## Validation Checklist

- [ ] Tenant CRD defined and operator running
- [ ] `kubectl apply` Tenant CR -> namespace auto-created
- [ ] ResourceQuota applied based on plan (free < starter < business < enterprise)
- [ ] LimitRange prevents any single pod from consuming all quota
- [ ] NetworkPolicies: tenant-a pods CANNOT reach tenant-b pods
- [ ] Tenant-aware middleware extracts tenant from header/subdomain
- [ ] Each tenant has own database (connection isolated)
- [ ] Gateway routes requests to correct tenant namespace
- [ ] Noisy neighbor test: tenant-a at full CPU doesn't affect tenant-b response times
- [ ] vcluster: enterprise tenant gets virtual cluster with cluster-admin
- [ ] Tenant onboarding: create Tenant CR -> fully functional in < 60 seconds
- [ ] Tenant deletion: cleanup namespace, database, secrets

---

## Key Concepts to Internalize

1. **Isolation spectrum**: from shared everything to dedicated everything. Choose based on requirements.
2. **Noisy neighbor**: one tenant's load shouldn't degrade others. ResourceQuotas + LimitRanges enforce this.
3. **Tenant context propagation**: tenant ID must flow through every service call, every database query.
4. **Operator pattern**: custom controller watches Tenant CRDs and reconciles desired state.
5. **vcluster**: virtual Kubernetes clusters within a host cluster. Best balance of isolation vs cost.
6. **Data isolation**: the hardest part. Schema-per-tenant is simple but limits flexibility. DB-per-tenant is safer.
