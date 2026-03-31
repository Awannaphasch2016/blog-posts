# Project 11: Content Moderation & AI Safety Policies
# TIER 3 -- Expert Platform Engineering

## Goal
Implement comprehensive security: OPA Gatekeeper for admission control,
HashiCorp Vault for secrets management, NetworkPolicies for micro-segmentation,
and RBAC with least-privilege service accounts.

---

## Architecture

```
  [kubectl apply / ArgoCD sync]
            |
  [OPA Gatekeeper] <-- Admission webhook
  "Does this manifest comply with policies?"
            |
     pass   |   deny (+ reason)
            |
  [Kubernetes API]
            |
  [Pod with Vault Agent Sidecar]
            |
  Vault Agent injects secrets as files
            |
  [Application reads secrets from /vault/secrets/]
            |
  [NetworkPolicy: only allowed traffic flows]
```

---

## Directory Structure

```
11-policy-security/
├── gatekeeper/
│   ├── install.sh
│   ├── constraint-templates/
│   │   ├── required-labels.yaml
│   │   ├── allowed-registries.yaml
│   │   ├── require-resource-limits.yaml
│   │   ├── block-privileged.yaml
│   │   ├── require-probes.yaml
│   │   └── block-latest-tag.yaml
│   └── constraints/
│       ├── require-team-label.yaml
│       ├── allow-only-internal-registry.yaml
│       ├── enforce-resource-limits.yaml
│       ├── no-privileged-containers.yaml
│       ├── require-liveness-probe.yaml
│       └── no-latest-tag.yaml
├── vault/
│   ├── install.sh
│   ├── vault-values.yaml
│   ├── policies/
│   │   ├── order-service-policy.hcl
│   │   ├── payment-service-policy.hcl
│   │   └── admin-policy.hcl
│   ├── k8s-auth/
│   │   └── setup-auth.sh
│   ├── secrets/
│   │   └── seed-secrets.sh
│   └── dynamic-secrets/
│       └── postgres-config.sh
├── network-policies/
│   ├── default-deny.yaml
│   ├── allow-dns.yaml
│   ├── order-to-payment.yaml
│   ├── services-to-databases.yaml
│   └── ingress-to-services.yaml
├── rbac/
│   ├── service-accounts/
│   │   ├── order-service-sa.yaml
│   │   ├── payment-service-sa.yaml
│   │   └── monitoring-sa.yaml
│   ├── roles/
│   │   ├── service-role.yaml
│   │   └── monitoring-role.yaml
│   └── bindings/
│       ├── order-service-binding.yaml
│       └── monitoring-binding.yaml
└── testing/
    ├── test-gatekeeper.sh
    ├── test-vault.sh
    └── test-network-policies.sh
```

---

## Step-by-Step Implementation

### Phase 1: OPA Gatekeeper (Day 1-4)

```bash
# Install Gatekeeper
helm repo add gatekeeper https://open-policy-agent.github.io/gatekeeper/charts
helm install gatekeeper gatekeeper/gatekeeper -n gatekeeper-system --create-namespace
```

**ConstraintTemplate (reusable policy logic in Rego):**
```yaml
# gatekeeper/constraint-templates/required-labels.yaml
apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata:
  name: k8srequiredlabels
spec:
  crd:
    spec:
      names:
        kind: K8sRequiredLabels
      validation:
        openAPIV3Schema:
          type: object
          properties:
            labels:
              type: array
              items:
                type: string
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8srequiredlabels

        violation[{"msg": msg}] {
          provided := {label | input.review.object.metadata.labels[label]}
          required := {label | label := input.parameters.labels[_]}
          missing := required - provided
          count(missing) > 0
          msg := sprintf("Missing required labels: %v", [missing])
        }
```

```yaml
# gatekeeper/constraint-templates/allowed-registries.yaml
apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata:
  name: k8sallowedregistries
spec:
  crd:
    spec:
      names:
        kind: K8sAllowedRegistries
      validation:
        openAPIV3Schema:
          type: object
          properties:
            registries:
              type: array
              items:
                type: string
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8sallowedregistries

        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          not startswith_any(container.image, input.parameters.registries)
          msg := sprintf("Container '%v' uses image '%v' from an untrusted registry. Allowed: %v",
            [container.name, container.image, input.parameters.registries])
        }

        violation[{"msg": msg}] {
          container := input.review.object.spec.initContainers[_]
          not startswith_any(container.image, input.parameters.registries)
          msg := sprintf("Init container '%v' uses image '%v' from an untrusted registry",
            [container.name, container.image])
        }

        startswith_any(str, prefixes) {
          prefix := prefixes[_]
          startswith(str, prefix)
        }
```

```yaml
# gatekeeper/constraint-templates/require-resource-limits.yaml
apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata:
  name: k8srequireresourcelimits
spec:
  crd:
    spec:
      names:
        kind: K8sRequireResourceLimits
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8srequireresourcelimits

        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          not container.resources.limits.cpu
          msg := sprintf("Container '%v' must have CPU limits", [container.name])
        }

        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          not container.resources.limits.memory
          msg := sprintf("Container '%v' must have memory limits", [container.name])
        }

        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          not container.resources.requests.cpu
          msg := sprintf("Container '%v' must have CPU requests", [container.name])
        }

        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          not container.resources.requests.memory
          msg := sprintf("Container '%v' must have memory requests", [container.name])
        }
```

**Constraints (apply templates to specific resources):**
```yaml
# gatekeeper/constraints/require-team-label.yaml
apiVersion: constraints.gatekeeper.sh/v1beta1
kind: K8sRequiredLabels
metadata:
  name: require-team-label
spec:
  match:
    kinds:
      - apiGroups: ["apps"]
        kinds: ["Deployment"]
    namespaces: ["ecommerce"]
  parameters:
    labels:
      - "team"
      - "app.kubernetes.io/name"
      - "app.kubernetes.io/version"

---
# gatekeeper/constraints/no-latest-tag.yaml
apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata:
  name: k8sblocklatestag
spec:
  crd:
    spec:
      names:
        kind: K8sBlockLatestTag
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8sblocklatestag
        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          endswith(container.image, ":latest")
          msg := sprintf("Container '%v' uses ':latest' tag. Use specific version tags.", [container.name])
        }
        violation[{"msg": msg}] {
          container := input.review.object.spec.containers[_]
          not contains(container.image, ":")
          msg := sprintf("Container '%v' has no tag. Use specific version tags.", [container.name])
        }
```

### Phase 2: HashiCorp Vault (Day 5-8)

```bash
# Install Vault via Helm
helm repo add hashicorp https://helm.releases.hashicorp.com
helm install vault hashicorp/vault -n vault --create-namespace \
  --set "server.dev.enabled=true" \
  --set "injector.enabled=true"
```

**Configure Kubernetes auth method:**
```bash
# vault/k8s-auth/setup-auth.sh
#!/bin/bash

# Enable K8s auth
kubectl exec -n vault vault-0 -- vault auth enable kubernetes

# Configure K8s auth with cluster info
kubectl exec -n vault vault-0 -- vault write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.default.svc:443"

# Create policy for order-service
kubectl exec -n vault vault-0 -- vault policy write order-service - <<EOF
path "secret/data/ecommerce/order-service/*" {
  capabilities = ["read"]
}
path "database/creds/order-service" {
  capabilities = ["read"]
}
EOF

# Create policy for payment-service (more restrictive)
kubectl exec -n vault vault-0 -- vault policy write payment-service - <<EOF
path "secret/data/ecommerce/payment-service/*" {
  capabilities = ["read"]
}
path "database/creds/payment-service" {
  capabilities = ["read"]
}
path "transit/encrypt/payment-key" {
  capabilities = ["update"]
}
path "transit/decrypt/payment-key" {
  capabilities = ["update"]
}
EOF

# Bind K8s service account to Vault role
kubectl exec -n vault vault-0 -- vault write auth/kubernetes/role/order-service \
  bound_service_account_names=order-service \
  bound_service_account_namespaces=ecommerce \
  policies=order-service \
  ttl=1h

kubectl exec -n vault vault-0 -- vault write auth/kubernetes/role/payment-service \
  bound_service_account_names=payment-service \
  bound_service_account_namespaces=ecommerce \
  policies=payment-service \
  ttl=1h
```

**Dynamic database credentials:**
```bash
# vault/dynamic-secrets/postgres-config.sh
#!/bin/bash

# Enable database secrets engine
kubectl exec -n vault vault-0 -- vault secrets enable database

# Configure PostgreSQL connection
kubectl exec -n vault vault-0 -- vault write database/config/orderdb \
  plugin_name=postgresql-database-plugin \
  allowed_roles="order-service" \
  connection_url="postgresql://{{username}}:{{password}}@postgres.ecommerce:5432/orderdb?sslmode=disable" \
  username="vault_admin" \
  password="vault_admin_password"

# Create role: generates credentials with 1h TTL
kubectl exec -n vault vault-0 -- vault write database/roles/order-service \
  db_name=orderdb \
  creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; \
    GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO \"{{name}}\";" \
  revocation_statements="REVOKE ALL ON ALL TABLES IN SCHEMA public FROM \"{{name}}\"; DROP ROLE IF EXISTS \"{{name}}\";" \
  default_ttl="1h" \
  max_ttl="24h"

# Test: get dynamic credentials
kubectl exec -n vault vault-0 -- vault read database/creds/order-service
# Returns: username=v-k8s-order-s-xxxxx, password=yyyyyyy, ttl=3600
```

**Pod with Vault Agent sidecar injection:**
```yaml
# Updated order-service deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
  namespace: ecommerce
spec:
  template:
    metadata:
      annotations:
        vault.hashicorp.com/agent-inject: "true"
        vault.hashicorp.com/role: "order-service"
        # Static secrets
        vault.hashicorp.com/agent-inject-secret-config: "secret/data/ecommerce/order-service/config"
        vault.hashicorp.com/agent-inject-template-config: |
          {{- with secret "secret/data/ecommerce/order-service/config" -}}
          export API_KEY="{{ .Data.data.api_key }}"
          export ENCRYPTION_KEY="{{ .Data.data.encryption_key }}"
          {{- end }}
        # Dynamic database credentials
        vault.hashicorp.com/agent-inject-secret-db: "database/creds/order-service"
        vault.hashicorp.com/agent-inject-template-db: |
          {{- with secret "database/creds/order-service" -}}
          export DATABASE_URL="postgresql://{{ .Data.username }}:{{ .Data.password }}@postgres.ecommerce:5432/orderdb"
          {{- end }}
    spec:
      serviceAccountName: order-service
      containers:
        - name: order-service
          image: order-service:v1
          command: ["/bin/sh", "-c"]
          args:
            - "source /vault/secrets/config && source /vault/secrets/db && /order-service"
```

### Phase 3: NetworkPolicies (Day 9-10)

```yaml
# network-policies/default-deny.yaml
# Start with deny-all, then explicitly allow
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: ecommerce
spec:
  podSelector: {}
  policyTypes:
    - Ingress
    - Egress

---
# network-policies/allow-dns.yaml
# All pods need DNS
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-dns
  namespace: ecommerce
spec:
  podSelector: {}
  policyTypes:
    - Egress
  egress:
    - to: []
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53

---
# network-policies/order-to-payment.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: order-to-payment
  namespace: ecommerce
spec:
  podSelector:
    matchLabels:
      app: payment-service
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: order-service
      ports:
        - protocol: TCP
          port: 8005
        - protocol: TCP
          port: 50051

---
# network-policies/services-to-databases.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: order-service-to-postgres
  namespace: ecommerce
spec:
  podSelector:
    matchLabels:
      app: postgres
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: order-service
        - podSelector:
            matchLabels:
              app: payment-service
      ports:
        - protocol: TCP
          port: 5432
```

### Phase 4: RBAC (Day 11)

```yaml
# rbac/service-accounts/order-service-sa.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: order-service
  namespace: ecommerce

---
# rbac/roles/service-role.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: service-reader
  namespace: ecommerce
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: []                       # No direct secret access! Use Vault.

---
# rbac/bindings/order-service-binding.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: order-service-reader
  namespace: ecommerce
subjects:
  - kind: ServiceAccount
    name: order-service
    namespace: ecommerce
roleRef:
  kind: Role
  name: service-reader
  apiGroup: rbac.authorization.k8s.io
```

---

## Testing

```bash
# testing/test-gatekeeper.sh
#!/bin/bash

# Test: deploy without required labels (should FAIL)
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bad-deploy
  namespace: ecommerce
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bad-deploy
  template:
    metadata:
      labels:
        app: bad-deploy
    spec:
      containers:
        - name: nginx
          image: nginx:latest
          # No resource limits!
EOF
# Expected: DENIED by Gatekeeper (missing labels, latest tag, no resource limits)

# Test: deploy with everything correct (should PASS)
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: good-deploy
  namespace: ecommerce
  labels:
    team: platform
    app.kubernetes.io/name: good-deploy
    app.kubernetes.io/version: "1.0.0"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: good-deploy
  template:
    metadata:
      labels:
        app: good-deploy
    spec:
      containers:
        - name: app
          image: internal-registry/good-deploy:1.0.0
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 250m
              memory: 256Mi
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
EOF
```

---

## Validation Checklist

- [ ] Gatekeeper blocks deployments without required labels
- [ ] Gatekeeper blocks images from untrusted registries
- [ ] Gatekeeper blocks containers without resource limits
- [ ] Gatekeeper blocks privileged containers
- [ ] Gatekeeper blocks `:latest` tag
- [ ] Vault running with K8s auth enabled
- [ ] Each service has its own Vault policy (least privilege)
- [ ] Dynamic DB credentials generated with TTL
- [ ] Vault Agent sidecar injects secrets into pods
- [ ] NetworkPolicies: default deny + explicit allow
- [ ] order-service can reach payment-service (allowed)
- [ ] product-service CANNOT reach payment-service (denied)
- [ ] RBAC: service accounts have minimal permissions

---

## Key Concepts to Internalize

1. **Defense in depth**: multiple layers (Gatekeeper + Vault + NetworkPolicies + RBAC)
2. **Policy as code**: Rego policies version-controlled, reviewed, tested like app code
3. **Dynamic secrets**: short-lived credentials that auto-rotate. No long-lived passwords.
4. **Zero-trust networking**: deny all, allow explicitly. Identity-based, not IP-based.
5. **Least privilege**: each service only accesses what it needs. Nothing more.
6. **Admission control**: prevent bad configs from ever reaching the cluster
