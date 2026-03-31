# Project 14: AI Compute & WebSocket Autoscaling
# TIER 3 -- Expert Platform Engineering

## Goal
Implement multi-dimensional autoscaling: HPA on custom Prometheus metrics,
VPA for right-sizing, KEDA for event-driven scaling (including scale-to-zero).
Load test and tune scaling behavior.

---

## Architecture

```
  [Prometheus]
       |
  custom metrics (req/s, consumer lag)
       |
  +----+----+----+
  |         |         |
 [HPA]    [VPA]    [KEDA]
  |         |         |
 Scale     Right-    Event-driven
 pods on   size      scale (Kafka
 req/s     resource  lag, HTTP rate)
           requests  + scale-to-zero
```

---

## Directory Structure

```
14-autoscaling/
├── hpa/
│   ├── custom-metrics/
│   │   ├── prometheus-adapter-values.yaml
│   │   └── custom-metrics-config.yaml
│   ├── order-service-hpa.yaml
│   ├── user-service-hpa.yaml
│   └── payment-service-hpa.yaml
├── vpa/
│   ├── install.sh
│   ├── order-service-vpa.yaml
│   └── recommendation-only.yaml    # Start with recommend mode
├── keda/
│   ├── scaled-objects/
│   │   ├── kafka-consumer-scaler.yaml
│   │   ├── http-scaler.yaml
│   │   └── cron-scaler.yaml
│   ├── trigger-auth/
│   │   └── kafka-auth.yaml
│   └── scale-to-zero-demo.yaml
├── load-testing/
│   ├── k6/
│   │   ├── ramp-up.js              # Gradual increase
│   │   ├── spike.js                # Sudden spike
│   │   ├── soak.js                 # Sustained load
│   │   └── stress.js               # Find breaking point
│   └── run-all-tests.sh
├── tuning/
│   ├── scaling-analysis.md
│   └── cost-optimization.md
└── dashboards/
    └── autoscaling-dashboard.json
```

---

## Step-by-Step Implementation

### Phase 1: HPA with Custom Prometheus Metrics (Day 1-4)

```bash
# Install Prometheus Adapter (bridges Prometheus -> K8s custom metrics API)
helm install prometheus-adapter prometheus-community/prometheus-adapter \
  -n observability \
  -f hpa/custom-metrics/prometheus-adapter-values.yaml
```

```yaml
# hpa/custom-metrics/prometheus-adapter-values.yaml
prometheus:
  url: http://prometheus-kube-prometheus-prometheus.observability
  port: 9090

rules:
  custom:
    # Expose requests-per-second as a K8s custom metric
    - seriesQuery: 'http_server_request_count{namespace!="",pod!=""}'
      resources:
        overrides:
          namespace: {resource: "namespace"}
          pod: {resource: "pod"}
      name:
        matches: "^(.*)_count$"
        as: "${1}_per_second"
      metricsQuery: 'sum(rate(<<.Series>>{<<.LabelMatchers>>}[2m])) by (<<.GroupBy>>)'

    # Expose active connections
    - seriesQuery: 'http_server_active_requests{namespace!="",pod!=""}'
      resources:
        overrides:
          namespace: {resource: "namespace"}
          pod: {resource: "pod"}
      name:
        as: "http_active_requests"
      metricsQuery: 'sum(<<.Series>>{<<.LabelMatchers>>}) by (<<.GroupBy>>)'
```

```yaml
# hpa/order-service-hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: order-service-hpa
  namespace: ecommerce
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-service
  minReplicas: 2
  maxReplicas: 20
  metrics:
    # Primary: custom metric (requests per second)
    - type: Pods
      pods:
        metric:
          name: http_server_request_per_second
        target:
          type: AverageValue
          averageValue: "50"      # Scale when > 50 req/s per pod

    # Secondary: CPU as safety net
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70

    # Tertiary: memory
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80

  behavior:
    scaleUp:
      stabilizationWindowSeconds: 30    # React quickly
      policies:
        - type: Percent
          value: 100                    # Double the pods
          periodSeconds: 60
        - type: Pods
          value: 4                      # Or add 4 pods
          periodSeconds: 60
      selectPolicy: Max                 # Use whichever adds more

    scaleDown:
      stabilizationWindowSeconds: 300   # Wait 5 min before scaling down
      policies:
        - type: Percent
          value: 25                     # Remove 25% at a time
          periodSeconds: 120
      selectPolicy: Min                 # Conservative scale-down
```

```bash
# Verify custom metrics are available
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | jq .
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/ecommerce/pods/*/http_server_request_per_second" | jq .
```

### Phase 2: Vertical Pod Autoscaler (Day 5-6)

```bash
# Install VPA
git clone https://github.com/kubernetes/autoscaler.git
cd autoscaler/vertical-pod-autoscaler
./hack/vpa-up.sh
```

```yaml
# vpa/recommendation-only.yaml
# Start in "Off" mode: VPA recommends but doesn't change anything
# Use this to understand actual resource usage before enabling auto-updates
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: order-service-vpa
  namespace: ecommerce
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-service
  updatePolicy:
    updateMode: "Off"            # Just recommend, don't auto-apply
  resourcePolicy:
    containerPolicies:
      - containerName: order-service
        minAllowed:
          cpu: 50m
          memory: 64Mi
        maxAllowed:
          cpu: 2
          memory: 2Gi

---
# After reviewing recommendations, switch to Auto:
# vpa/order-service-vpa.yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: order-service-vpa
  namespace: ecommerce
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-service
  updatePolicy:
    updateMode: "Auto"           # VPA will restart pods with right-sized resources
  resourcePolicy:
    containerPolicies:
      - containerName: order-service
        controlledResources: ["cpu", "memory"]
        minAllowed:
          cpu: 50m
          memory: 64Mi
        maxAllowed:
          cpu: 2
          memory: 2Gi
```

```bash
# Check VPA recommendations
kubectl describe vpa order-service-vpa -n ecommerce
# Look for:
#   Target: cpu=250m, memory=384Mi
#   Lower Bound: cpu=100m, memory=256Mi
#   Upper Bound: cpu=500m, memory=512Mi
```

**NOTE: VPA and HPA conflict on the same resource (CPU/memory). Use them together like this:**
- HPA scales based on custom metrics (req/s) -- controls replica count
- VPA right-sizes resource requests/limits -- controls pod size
- Don't let HPA scale on CPU if VPA is managing CPU requests

### Phase 3: KEDA Event-Driven Scaling (Day 7-9)

```yaml
# keda/scaled-objects/kafka-consumer-scaler.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: inventory-consumer-scaler
  namespace: ecommerce
spec:
  scaleTargetRef:
    name: inventory-service
  pollingInterval: 15
  cooldownPeriod: 60
  minReplicaCount: 1
  maxReplicaCount: 20
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 15
          policies:
            - type: Percent
              value: 100
              periodSeconds: 30
        scaleDown:
          stabilizationWindowSeconds: 120
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: ecommerce-cluster-kafka-bootstrap.kafka:9092
        consumerGroup: inventory-service
        topic: order.events
        lagThreshold: "100"           # Scale up when lag > 100
        activationLagThreshold: "0"

---
# keda/scaled-objects/http-scaler.yaml
# Scale based on HTTP request rate from Prometheus
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: user-service-http-scaler
  namespace: ecommerce
spec:
  scaleTargetRef:
    name: user-service
  pollingInterval: 15
  cooldownPeriod: 120
  minReplicaCount: 1
  maxReplicaCount: 15
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus-kube-prometheus-prometheus.observability:9090
        metricName: http_requests_per_second
        query: |
          sum(rate(http_server_request_count{service_name="user-service"}[2m]))
        threshold: "200"              # Scale when total > 200 req/s
        activationThreshold: "10"     # Scale from 0 when > 10 req/s

---
# keda/scale-to-zero-demo.yaml
# Non-critical services can scale to zero
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: notification-scale-to-zero
  namespace: ecommerce
spec:
  scaleTargetRef:
    name: notification-service
  pollingInterval: 30
  cooldownPeriod: 300                  # Wait 5 min before scaling to 0
  minReplicaCount: 0                   # Scale to zero!
  maxReplicaCount: 5
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: ecommerce-cluster-kafka-bootstrap.kafka:9092
        consumerGroup: notification-service
        topic: notification.events
        lagThreshold: "5"
        activationLagThreshold: "1"   # Wake up on first message

---
# keda/scaled-objects/cron-scaler.yaml
# Pre-scale before known traffic peaks
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: order-service-cron-scaler
  namespace: ecommerce
spec:
  scaleTargetRef:
    name: order-service
  triggers:
    - type: cron
      metadata:
        timezone: America/New_York
        start: "0 8 * * 1-5"         # Mon-Fri 8 AM
        end: "0 20 * * 1-5"          # Mon-Fri 8 PM
        desiredReplicas: "5"          # Pre-scale for business hours
    - type: cron
      metadata:
        timezone: America/New_York
        start: "0 20 * * 1-5"        # After hours
        end: "0 8 * * 2-6"
        desiredReplicas: "2"          # Scale down at night
```

### Phase 4: Load Testing Suite (Day 10-11)

```javascript
// load-testing/k6/ramp-up.js
// Gradual ramp: observe scaling behavior
import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  stages: [
    { duration: '2m', target: 10 },    // Warm up
    { duration: '3m', target: 50 },    // Low load
    { duration: '3m', target: 100 },   // Medium load
    { duration: '3m', target: 200 },   // High load
    { duration: '3m', target: 500 },   // Peak load
    { duration: '5m', target: 500 },   // Sustain peak
    { duration: '3m', target: 50 },    // Scale down
    { duration: '2m', target: 0 },     // Cool down
  ],
};

export default function () {
  http.get('http://localhost:8080/api/v1/products');
  sleep(0.1);
}

// load-testing/k6/spike.js
// Sudden spike: test scaling speed
export const options = {
  stages: [
    { duration: '1m', target: 10 },    // Baseline
    { duration: '10s', target: 500 },   // SPIKE!
    { duration: '5m', target: 500 },   // Sustain
    { duration: '10s', target: 10 },    // Drop
    { duration: '3m', target: 10 },    // Recover
  ],
};

// load-testing/k6/soak.js
// Long sustained load: find memory leaks, connection leaks
export const options = {
  stages: [
    { duration: '2m', target: 100 },
    { duration: '4h', target: 100 },   // 4 hours sustained
    { duration: '2m', target: 0 },
  ],
};
```

### Phase 5: Analyze and Tune (Day 12)

```markdown
# tuning/scaling-analysis.md

## What to Measure During Load Tests

1. **Time to scale up**: from trigger -> new pod ready (target: < 60s)
2. **Time to scale down**: from low load -> pods removed (target: 5-10 min)
3. **Scaling oscillation**: pods added/removed/added rapidly (BAD)
4. **Resource waste**: over-provisioned pods sitting idle
5. **Request errors during scaling**: connection errors during pod churn

## Tuning Knobs

### HPA
- `stabilizationWindowSeconds`: higher = less oscillation, slower response
- `scaleUp.policies`: control HOW FAST to scale up
- `scaleDown.policies`: control HOW SLOW to scale down (be conservative)
- `selectPolicy: Max/Min`: max for aggressive scale-up, min for conservative scale-down

### KEDA
- `pollingInterval`: how often to check metrics (lower = faster reaction)
- `cooldownPeriod`: wait before scaling to zero (avoid constant wake/sleep)
- `lagThreshold`: Kafka consumer lag target (lower = more aggressive scaling)

### VPA
- Compare recommendations to actual resource requests
- Look at OOM kills (memory too low) and throttling (CPU too low)
- Min/max bounds prevent unreasonable recommendations

## Common Issues
- **Scaling too slow**: reduce stabilizationWindowSeconds, increase scaleUp percent
- **Scaling oscillation**: increase stabilizationWindowSeconds, use Min selectPolicy for scale-down
- **Scale-to-zero cold start**: pre-warm with cron trigger, optimize container startup time
- **HPA + VPA conflict**: use HPA for replica count (custom metrics), VPA for pod sizing
```

---

## Validation Checklist

- [ ] Prometheus Adapter exposes custom metrics to K8s API
- [ ] HPA scales on requests-per-second (not just CPU)
- [ ] HPA behavior: fast scale-up (30s window), slow scale-down (5min window)
- [ ] VPA recommendations visible (`kubectl describe vpa`)
- [ ] VPA in Auto mode right-sizes pods (compare before/after resource requests)
- [ ] KEDA scales Kafka consumers based on consumer lag
- [ ] KEDA scale-to-zero works (notification-service → 0 pods when idle)
- [ ] KEDA wakes from zero within 30 seconds of new messages
- [ ] Cron scaler pre-scales before business hours
- [ ] Ramp-up test: pods scale smoothly from 2 → 20
- [ ] Spike test: system handles sudden 50x increase
- [ ] Soak test: no memory leaks, no connection leaks after 4 hours
- [ ] No scaling oscillation (pods not flip-flopping)
- [ ] Grafana dashboard shows: replica count, HPA metrics, KEDA triggers, VPA recommendations

---

## Key Concepts to Internalize

1. **Scale-up fast, scale-down slow**: overpaying for a few minutes is fine. Dropping traffic is not.
2. **Custom metrics > CPU**: CPU is a lagging indicator. Scale on business metrics (req/s, queue depth).
3. **Scale-to-zero**: game changer for cost. But cold start must be fast (optimize container startup).
4. **HPA + VPA**: complementary. HPA controls HOW MANY pods, VPA controls HOW BIG each pod is.
5. **KEDA**: bridges event sources (Kafka, HTTP, cron, custom) to K8s scaling. The "glue" layer.
6. **Load test before production**: you can't tune what you haven't measured.
