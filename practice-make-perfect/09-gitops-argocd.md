# Project 09: GitOps for AI Model Deployments
# TIER 2 -- Production Patterns

## Goal
Implement a full GitOps pipeline with ArgoCD for declarative AI model deployments.
Add Argo Rollouts for progressive AI model delivery: canary deployments with
automated AI performance metric analysis (latency, accuracy, cost) and
automated rollback when AI model performance degrades.

---

## Architecture

```
  [Developer]
       |
  git push
       |
  [Git Repository] <--- single source of truth
       |
  [ArgoCD] (watches repo, syncs to cluster)
       |
  [Argo Rollouts]
       |
  canary 10% -> 30% -> 60% -> 100%
       |
  [Prometheus] (automated metric analysis at each step)
       |
  pass? -> promote | fail? -> rollback
```

---

## Directory Structure

```
09-gitops-argocd/
├── gitops-repo/                    # This IS the Git repo ArgoCD watches
│   ├── apps/                       # ArgoCD Application definitions
│   │   ├── root-app.yaml          # App-of-apps pattern
│   │   ├── user-service.yaml
│   │   ├── order-service.yaml
│   │   ├── payment-service.yaml
│   │   └── infrastructure.yaml
│   ├── base/                       # Kustomize base manifests
│   │   ├── user-service/
│   │   │   ├── deployment.yaml
│   │   │   ├── service.yaml
│   │   │   ├── hpa.yaml
│   │   │   └── kustomization.yaml
│   │   ├── order-service/
│   │   └── payment-service/
│   ├── overlays/                   # Environment-specific overrides
│   │   ├── dev/
│   │   │   ├── kustomization.yaml
│   │   │   ├── replicas-patch.yaml
│   │   │   └── resources-patch.yaml
│   │   ├── staging/
│   │   │   ├── kustomization.yaml
│   │   │   └── replicas-patch.yaml
│   │   └── prod/
│   │       ├── kustomization.yaml
│   │       ├── replicas-patch.yaml
│   │       └── resources-patch.yaml
│   ├── rollouts/                   # Argo Rollouts configs
│   │   ├── order-service-rollout.yaml
│   │   └── analysis-templates/
│   │       ├── success-rate.yaml
│   │       └── latency.yaml
│   └── secrets/
│       └── sealed-secrets/
│           ├── db-credentials.yaml
│           └── api-keys.yaml
├── argocd/
│   ├── install.sh
│   ├── argocd-values.yaml
│   └── projects/
│       └── ecommerce-project.yaml
└── scripts/
    ├── seal-secret.sh
    ├── promote-canary.sh
    └── rollback.sh
```

---

## Step-by-Step Implementation

### Phase 1: Install ArgoCD (Day 1-2)

```bash
# Install ArgoCD
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Wait for it
kubectl wait deployment/argocd-server -n argocd --for=condition=Available --timeout=300s

# Get admin password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d

# Port forward the UI
kubectl port-forward svc/argocd-server -n argocd 8443:443

# Install CLI
curl -sSL -o argocd https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64
chmod +x argocd && sudo mv argocd /usr/local/bin/

# Login
argocd login localhost:8443 --insecure

# Install Argo Rollouts
kubectl create namespace argo-rollouts
kubectl apply -n argo-rollouts -f https://github.com/argoproj/argo-rollouts/releases/latest/download/install.yaml

# Install Argo Rollouts kubectl plugin
curl -LO https://github.com/argoproj/argo-rollouts/releases/latest/download/kubectl-argo-rollouts-linux-amd64
chmod +x kubectl-argo-rollouts-linux-amd64
sudo mv kubectl-argo-rollouts-linux-amd64 /usr/local/bin/kubectl-argo-rollouts
```

### Phase 2: Kustomize Base + Overlays (Day 3-4)

```yaml
# gitops-repo/base/order-service/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
  labels:
    app: order-service
spec:
  selector:
    matchLabels:
      app: order-service
  template:
    metadata:
      labels:
        app: order-service
    spec:
      containers:
        - name: order-service
          image: order-service:latest
          ports:
            - containerPort: 8004
          envFrom:
            - configMapRef:
                name: order-service-config
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 250m
              memory: 256Mi
---
# gitops-repo/base/order-service/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
  - hpa.yaml
commonLabels:
  app.kubernetes.io/part-of: ecommerce
```

```yaml
# gitops-repo/overlays/dev/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ecommerce-dev
resources:
  - ../../base/order-service
  - ../../base/user-service
  - ../../base/payment-service
patches:
  - path: replicas-patch.yaml
  - path: resources-patch.yaml

---
# gitops-repo/overlays/dev/replicas-patch.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 1    # Dev: single replica
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: user-service
spec:
  replicas: 1
```

```yaml
# gitops-repo/overlays/prod/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ecommerce-prod
resources:
  - ../../base/order-service
  - ../../base/user-service
  - ../../base/payment-service
patches:
  - path: replicas-patch.yaml

---
# gitops-repo/overlays/prod/replicas-patch.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 3    # Prod: 3 replicas minimum
```

### Phase 3: ArgoCD Application Definitions (Day 5-6)

```yaml
# gitops-repo/apps/root-app.yaml
# App-of-apps: one Application that manages all other Applications
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: root
  namespace: argocd
spec:
  project: ecommerce
  source:
    repoURL: https://github.com/youruser/ecommerce-gitops
    targetRevision: main
    path: apps
  destination:
    server: https://kubernetes.default.svc
    namespace: argocd
  syncPolicy:
    automated:
      prune: true
      selfHeal: true

---
# gitops-repo/apps/order-service.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: order-service
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: ecommerce
  source:
    repoURL: https://github.com/youruser/ecommerce-gitops
    targetRevision: main
    path: overlays/dev
  destination:
    server: https://kubernetes.default.svc
    namespace: ecommerce
  syncPolicy:
    automated:
      prune: true       # Delete resources removed from Git
      selfHeal: true    # Revert manual changes
    syncOptions:
      - CreateNamespace=true
    retry:
      limit: 3
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 1m
```

### Phase 4: Argo Rollouts -- Canary with Metrics (Day 7-9)

```yaml
# gitops-repo/rollouts/order-service-rollout.yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: order-service
  namespace: ecommerce
spec:
  replicas: 5
  selector:
    matchLabels:
      app: order-service
  template:
    metadata:
      labels:
        app: order-service
    spec:
      containers:
        - name: order-service
          image: order-service:v2    # New version
          ports:
            - containerPort: 8004
  strategy:
    canary:
      canaryService: order-service-canary
      stableService: order-service-stable
      trafficRouting:
        istio:
          virtualService:
            name: order-service
            routes:
              - primary
      steps:
        # Step 1: 10% traffic to canary, run analysis
        - setWeight: 10
        - analysis:
            templates:
              - templateName: success-rate
              - templateName: latency-check
            args:
              - name: service-name
                value: order-service-canary
        # Step 2: 30% if metrics pass
        - setWeight: 30
        - pause: { duration: 2m }
        # Step 3: 60%
        - setWeight: 60
        - analysis:
            templates:
              - templateName: success-rate
        - pause: { duration: 2m }
        # Step 4: 100% -- full promotion
        - setWeight: 100
      # Auto rollback on failure
      abortScaleDownDelaySeconds: 30
```

```yaml
# gitops-repo/rollouts/analysis-templates/success-rate.yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: success-rate
  namespace: ecommerce
spec:
  args:
    - name: service-name
  metrics:
    - name: success-rate
      interval: 30s
      count: 5
      successCondition: result[0] >= 0.95    # 95%+ success rate
      failureLimit: 3
      provider:
        prometheus:
          address: http://prometheus-kube-prometheus-prometheus.observability:9090
          query: |
            sum(rate(http_server_request_count{service_name="{{args.service-name}}",http_status_code!~"5.."}[2m]))
            /
            sum(rate(http_server_request_count{service_name="{{args.service-name}}"}[2m]))

---
# gitops-repo/rollouts/analysis-templates/latency.yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: latency-check
  namespace: ecommerce
spec:
  args:
    - name: service-name
  metrics:
    - name: p99-latency
      interval: 30s
      count: 5
      successCondition: result[0] < 2000     # p99 < 2 seconds
      failureLimit: 3
      provider:
        prometheus:
          address: http://prometheus-kube-prometheus-prometheus.observability:9090
          query: |
            histogram_quantile(0.99,
              sum(rate(http_server_duration_bucket{service_name="{{args.service-name}}"}[2m])) by (le)
            )
```

### Phase 5: Sealed Secrets (Day 10)

```bash
# Install Sealed Secrets controller
helm repo add sealed-secrets https://bitnami-labs.github.io/sealed-secrets
helm install sealed-secrets sealed-secrets/sealed-secrets -n kube-system

# Install kubeseal CLI
KUBESEAL_VERSION=$(curl -s https://api.github.com/repos/bitnami-labs/sealed-secrets/tags | jq -r '.[0].name' | cut -c 2-)
curl -OL "https://github.com/bitnami-labs/sealed-secrets/releases/download/v${KUBESEAL_VERSION}/kubeseal-${KUBESEAL_VERSION}-linux-amd64.tar.gz"
tar -xvzf kubeseal-*.tar.gz kubeseal
sudo install -m 755 kubeseal /usr/local/bin/kubeseal
```

```bash
# scripts/seal-secret.sh
#!/bin/bash
# Create a secret, seal it, store sealed version in Git

# Create regular secret
kubectl create secret generic db-credentials \
  --namespace ecommerce \
  --from-literal=username=orderdb \
  --from-literal=password=supersecret \
  --dry-run=client -o yaml | \
  kubeseal --format yaml > gitops-repo/secrets/sealed-secrets/db-credentials.yaml

# The sealed secret is safe to commit to Git!
# Only the cluster's controller can decrypt it.
```

---

## Workflow: Deploying a New Version

```bash
# 1. Build and push new image
docker build -t order-service:v2 ./services/order-service
kind load docker-image order-service:v2 --name microservices-lab

# 2. Update the image tag in Git
cd gitops-repo
kustomize edit set image order-service=order-service:v2

# 3. Commit and push
git add . && git commit -m "deploy order-service v2" && git push

# 4. ArgoCD detects the change and starts sync
# 5. Argo Rollouts runs canary: 10% -> analysis -> 30% -> 60% -> 100%
# 6. If metrics fail at any step: automatic rollback to v1

# Watch the rollout
kubectl argo rollouts get rollout order-service -n ecommerce --watch
```

---

## Validation Checklist

- [ ] ArgoCD installed and UI accessible
- [ ] App-of-apps pattern: root app manages all service apps
- [ ] Kustomize overlays: dev (1 replica), prod (3 replicas)
- [ ] ArgoCD auto-syncs when Git changes (push -> deploy)
- [ ] ArgoCD self-heals (manual `kubectl edit` reverted)
- [ ] ArgoCD prunes (delete from Git -> deleted from cluster)
- [ ] Argo Rollouts canary: traffic shifts 10% -> 30% -> 60% -> 100%
- [ ] AnalysisTemplate queries Prometheus for success rate
- [ ] Bad deploy (introduce error): automatic rollback triggered
- [ ] Sealed Secrets: encrypted in Git, decrypted only in cluster
- [ ] `kubectl argo rollouts get rollout order-service --watch` shows progress

---

## Key Concepts to Internalize

1. **GitOps = Git is the single source of truth**. No `kubectl apply` manually. Ever.
2. **App-of-apps**: one root Application manages all others. Add a service = add a YAML file.
3. **Self-heal**: someone runs `kubectl scale`? ArgoCD reverts it to match Git.
4. **Progressive delivery**: don't ship to 100% at once. Canary catches issues early.
5. **Automated analysis**: Prometheus metrics decide promote vs rollback. No human in the loop.
6. **Sealed Secrets**: secrets in Git, but encrypted. Only the cluster can decrypt.
