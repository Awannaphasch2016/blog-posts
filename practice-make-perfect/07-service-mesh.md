# Project 07: Service Mesh for AI Service Security
# TIER 2 -- Production Patterns

## Goal
Add a service mesh to the chat cluster for automatic mTLS between AI services,
traffic management for AI model deployments, canary deployments for AI updates,
fault injection for resilience testing, and per-route observability for AI APIs.
Implement with both Linkerd (simple) and Istio (feature-rich).

---

## Architecture

```
  [Ingress]
      |
  [Service Mesh Control Plane]
      |
  +---+---+---+
  |       |       |
[Pod A] [Pod B] [Pod C]
  |sidecar| |sidecar| |sidecar|
  [Proxy]  [Proxy]  [Proxy]
      \       |       /
       mTLS encrypted
```

---

## Directory Structure

```
07-service-mesh/
├── linkerd/
│   ├── install.sh
│   ├── service-profiles/
│   │   ├── user-service-profile.yaml
│   │   ├── order-service-profile.yaml
│   │   └── payment-service-profile.yaml
│   ├── traffic-split/
│   │   └── canary-order-service.yaml
│   └── authorization/
│       └── payment-policy.yaml
├── istio/
│   ├── install.sh
│   ├── virtual-services/
│   │   ├── user-service-vs.yaml
│   │   ├── order-service-vs.yaml
│   │   └── canary-vs.yaml
│   ├── destination-rules/
│   │   ├── user-service-dr.yaml
│   │   └── circuit-breaker-dr.yaml
│   ├── fault-injection/
│   │   ├── delay-payment.yaml
│   │   └── abort-shipping.yaml
│   ├── authorization/
│   │   ├── deny-all.yaml
│   │   └── allow-specific.yaml
│   └── gateway/
│       └── ingress-gateway.yaml
├── testing/
│   ├── traffic-shifting-test.sh
│   ├── fault-injection-test.sh
│   └── mtls-verify.sh
└── dashboards/
    └── mesh-dashboard.json
```

---

## Step-by-Step Implementation

### Path A: Linkerd (Start Here -- Day 1-5)

```bash
# Install Linkerd CLI
curl --proto '=https' -sSfL https://run.linkerd.io/install | sh
export PATH=$HOME/.linkerd2/bin:$PATH

# Check prerequisites
linkerd check --pre

# Install CRDs and control plane
linkerd install --crds | kubectl apply -f -
linkerd install | kubectl apply -f -
linkerd check

# Install viz extension (dashboard + metrics)
linkerd viz install | kubectl apply -f -
linkerd viz check

# Inject sidecar proxies into ecommerce namespace
kubectl get deploy -n ecommerce -o yaml | linkerd inject - | kubectl apply -f -

# Verify injection
linkerd check --proxy -n ecommerce

# Open dashboard
linkerd viz dashboard
```

**Service Profiles (per-route metrics + retries):**
```yaml
# linkerd/service-profiles/order-service-profile.yaml
apiVersion: linkerd.io/v1alpha2
kind: ServiceProfile
metadata:
  name: order-service.ecommerce.svc.cluster.local
  namespace: ecommerce
spec:
  routes:
    - name: POST /api/v1/orders
      condition:
        method: POST
        pathRegex: /api/v1/orders
      isRetryable: false          # Don't retry order creation!
      timeout: 10s
    - name: GET /api/v1/orders/{id}
      condition:
        method: GET
        pathRegex: /api/v1/orders/[^/]+
      isRetryable: true
      timeout: 5s
    - name: GET /api/v1/orders
      condition:
        method: GET
        pathRegex: /api/v1/orders
      isRetryable: true
      timeout: 5s
```

**Traffic Splitting (canary):**
```yaml
# linkerd/traffic-split/canary-order-service.yaml
apiVersion: split.smi-spec.io/v1alpha2
kind: TrafficSplit
metadata:
  name: order-service-canary
  namespace: ecommerce
spec:
  service: order-service
  backends:
    - service: order-service-stable
      weight: 900       # 90% to stable
    - service: order-service-canary
      weight: 100       # 10% to canary
```

**Authorization Policy (mTLS-based):**
```yaml
# linkerd/authorization/payment-policy.yaml
apiVersion: policy.linkerd.io/v1beta3
kind: Server
metadata:
  name: payment-service
  namespace: ecommerce
spec:
  podSelector:
    matchLabels:
      app: payment-service
  port: 8005
  proxyProtocol: HTTP/2
---
apiVersion: policy.linkerd.io/v1beta3
kind: AuthorizationPolicy
metadata:
  name: only-order-service
  namespace: ecommerce
spec:
  targetRef:
    group: policy.linkerd.io
    kind: Server
    name: payment-service
  requiredAuthenticationRefs:
    - name: order-service-identity
      kind: MeshTLSAuthentication
      group: policy.linkerd.io
---
apiVersion: policy.linkerd.io/v1alpha1
kind: MeshTLSAuthentication
metadata:
  name: order-service-identity
  namespace: ecommerce
spec:
  identities:
    - "*.ecommerce.serviceaccount.identity.linkerd.cluster.local"
  # Only order-service SA can call payment-service
```

### Path B: Istio (Day 6-12)

```bash
# Install Istio
curl -L https://istio.io/downloadIstio | sh -
cd istio-*
export PATH=$PWD/bin:$PATH

# Install with demo profile (includes all components)
istioctl install --set profile=demo -y

# Enable sidecar injection for namespace
kubectl label namespace ecommerce istio-injection=enabled

# Restart all pods to get sidecars
kubectl rollout restart deployment -n ecommerce

# Verify
istioctl analyze -n ecommerce
```

**VirtualService for traffic management:**
```yaml
# istio/virtual-services/order-service-vs.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: order-service
  namespace: ecommerce
spec:
  hosts:
    - order-service
  http:
    - match:
        - headers:
            x-canary:
              exact: "true"
      route:
        - destination:
            host: order-service
            subset: canary
    - route:
        - destination:
            host: order-service
            subset: stable
          weight: 90
        - destination:
            host: order-service
            subset: canary
          weight: 10
      timeout: 10s
      retries:
        attempts: 3
        perTryTimeout: 3s
        retryOn: gateway-error,connect-failure,refused-stream
```

**DestinationRule with circuit breaker:**
```yaml
# istio/destination-rules/circuit-breaker-dr.yaml
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: order-service
  namespace: ecommerce
spec:
  host: order-service
  trafficPolicy:
    connectionPool:
      tcp:
        maxConnections: 100
      http:
        h2UpgradePolicy: DEFAULT
        http1MaxPendingRequests: 100
        http2MaxRequests: 1000
        maxRequestsPerConnection: 10
    outlierDetection:
      consecutive5xxErrors: 5
      interval: 30s
      baseEjectionTime: 30s
      maxEjectionPercent: 50
  subsets:
    - name: stable
      labels:
        version: v1
    - name: canary
      labels:
        version: v2
```

**Fault injection for testing:**
```yaml
# istio/fault-injection/delay-payment.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: payment-service-fault
  namespace: ecommerce
spec:
  hosts:
    - payment-service
  http:
    - fault:
        delay:
          percentage:
            value: 50      # 50% of requests
          fixedDelay: 5s   # 5 second delay
      route:
        - destination:
            host: payment-service
---
# istio/fault-injection/abort-shipping.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: shipping-service-fault
  namespace: ecommerce
spec:
  hosts:
    - shipping-service
  http:
    - fault:
        abort:
          percentage:
            value: 10      # 10% of requests return 503
          httpStatus: 503
      route:
        - destination:
            host: shipping-service
```

**Authorization policy (zero-trust):**
```yaml
# istio/authorization/deny-all.yaml
# Default: deny all traffic in namespace
apiVersion: security.istio.io/v1
kind: AuthorizationPolicy
metadata:
  name: deny-all
  namespace: ecommerce
spec:
  {}
---
# istio/authorization/allow-specific.yaml
# Allow specific service-to-service calls
apiVersion: security.istio.io/v1
kind: AuthorizationPolicy
metadata:
  name: allow-order-to-payment
  namespace: ecommerce
spec:
  selector:
    matchLabels:
      app: payment-service
  action: ALLOW
  rules:
    - from:
        - source:
            principals:
              - "cluster.local/ns/ecommerce/sa/order-service"
      to:
        - operation:
            methods: ["POST"]
            paths: ["/api/v1/payments"]
```

---

## Testing Scripts

```bash
# testing/mtls-verify.sh
# Verify all traffic is encrypted
#!/bin/bash

# Linkerd
linkerd viz edges deployment -n ecommerce
# Should show all connections with "secured" status

# Istio
istioctl proxy-config secret deployment/order-service -n ecommerce
# Should show active TLS certificates

# Try to connect without mTLS (should fail)
kubectl run curl-test --image=curlimages/curl -n default -- \
  curl -s http://payment-service.ecommerce:8005/api/v1/payments
# Should be DENIED
```

```bash
# testing/traffic-shifting-test.sh
#!/bin/bash

# Send 1000 requests and count which version responds
for i in $(seq 1 1000); do
  VERSION=$(curl -s http://localhost:8080/api/v1/orders/health | jq -r '.version')
  echo $VERSION
done | sort | uniq -c

# Expected output (roughly):
# 900 v1
# 100 v2
```

---

## Validation Checklist

### Linkerd
- [ ] All pods have linkerd-proxy sidecar injected
- [ ] `linkerd viz dashboard` shows topology and metrics
- [ ] mTLS enabled between all services (check with `linkerd viz edges`)
- [ ] Service profiles provide per-route metrics
- [ ] Traffic split sends 90/10 to stable/canary
- [ ] Authorization policy restricts who can call payment-service

### Istio
- [ ] All pods have istio-proxy sidecar
- [ ] VirtualService routes traffic with header-based and weight-based rules
- [ ] DestinationRule circuit breaker ejects unhealthy pods
- [ ] Fault injection adds delay to payment-service
- [ ] Fault injection aborts 10% of shipping requests
- [ ] Zero-trust: deny-all + explicit allow policies
- [ ] Kiali dashboard shows service graph and traffic flow
- [ ] Canary deployment shifts traffic gradually

---

## Key Concepts to Internalize

1. **Sidecar proxy**: intercepts all traffic, adds mTLS/retries/metrics without code changes
2. **mTLS**: mutual TLS authenticates both sides. Service mesh manages cert rotation.
3. **Traffic management in infrastructure**: retries, timeouts, circuit breaking move from app code to mesh config
4. **Fault injection**: test resilience without modifying application code
5. **Zero-trust networking**: deny all, then explicitly allow. Identity-based, not IP-based.
6. **Linkerd vs Istio**: Linkerd = simpler, lighter, faster. Istio = more features, more complexity. Choose based on needs.
