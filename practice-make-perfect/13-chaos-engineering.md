# Project 13: Chaos Engineering for Chat System Resilience
# TIER 3 -- Expert Platform Engineering

## Goal
Build confidence in system resilience by systematically injecting failures.
Use Chaos Mesh for Kubernetes-native experiments. Run GameDays combining
chaos with load testing to validate SLOs.

---

## Architecture

```
  [Chaos Mesh Dashboard]
         |
  [Chaos Controller Manager]
         |
  Injects faults into:
  +------+------+------+
  |      |      |      |
 Pods  Network  IO   Stress
 kill  delay   faults  CPU/mem
  |      |      |      |
  [Your microservices platform]
         |
  [Prometheus + Grafana]
  Observe: did SLOs hold?
```

---

## Directory Structure

```
13-chaos-engineering/
├── chaos-mesh/
│   ├── install.sh
│   └── dashboard-ingress.yaml
├── experiments/
│   ├── pod-chaos/
│   │   ├── pod-kill.yaml
│   │   ├── pod-failure.yaml
│   │   └── container-kill.yaml
│   ├── network-chaos/
│   │   ├── network-delay.yaml
│   │   ├── network-partition.yaml
│   │   ├── network-loss.yaml
│   │   └── network-bandwidth.yaml
│   ├── stress-chaos/
│   │   ├── cpu-stress.yaml
│   │   └── memory-stress.yaml
│   ├── io-chaos/
│   │   ├── io-delay.yaml
│   │   └── io-error.yaml
│   ├── kafka-chaos/
│   │   └── broker-kill.yaml
│   └── dns-chaos/
│       └── dns-error.yaml
├── gamedays/
│   ├── gameday-1-pod-resilience/
│   │   ├── README.md
│   │   ├── hypothesis.md
│   │   ├── experiment.yaml
│   │   ├── load-test.js
│   │   └── results.md
│   ├── gameday-2-network-partition/
│   ├── gameday-3-cascade-prevention/
│   └── gameday-4-data-plane/
├── slos/
│   ├── availability-slo.yaml       # PrometheusRule
│   ├── latency-slo.yaml
│   └── error-budget.yaml
├── load-testing/
│   ├── k6/
│   │   ├── order-flow.js
│   │   ├── browse-products.js
│   │   └── mixed-workload.js
│   └── run-load-test.sh
└── runbooks/
    ├── pod-crash-loop.md
    ├── high-latency.md
    └── database-connection-exhausted.md
```

---

## Step-by-Step Implementation

### Phase 1: Install Chaos Mesh (Day 1)

```bash
# Install Chaos Mesh
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm install chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock

# Verify
kubectl get pods -n chaos-mesh

# Access dashboard
kubectl port-forward svc/chaos-dashboard -n chaos-mesh 2333:2333
# Open http://localhost:2333
```

### Phase 2: Pod Chaos Experiments (Day 2-3)

```yaml
# experiments/pod-chaos/pod-kill.yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: kill-order-service
  namespace: ecommerce
spec:
  action: pod-kill
  mode: one                        # Kill one random pod
  selector:
    namespaces:
      - ecommerce
    labelSelectors:
      app: order-service
  scheduler:
    cron: "*/5 * * * *"            # Every 5 minutes
  duration: "60s"

---
# experiments/pod-chaos/pod-failure.yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: fail-payment-service
  namespace: ecommerce
spec:
  action: pod-failure
  mode: fixed-percent
  value: "50"                      # 50% of pods
  selector:
    namespaces:
      - ecommerce
    labelSelectors:
      app: payment-service
  duration: "120s"                  # Fail for 2 minutes
```

### Phase 3: Network Chaos (Day 4-5)

```yaml
# experiments/network-chaos/network-delay.yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: delay-to-database
  namespace: ecommerce
spec:
  action: delay
  mode: all
  selector:
    namespaces:
      - ecommerce
    labelSelectors:
      app: order-service
  delay:
    latency: "200ms"
    jitter: "50ms"
    correlation: "50"
  direction: to
  target:
    selector:
      namespaces:
        - ecommerce
      labelSelectors:
        app: postgres
    mode: all
  duration: "300s"

---
# experiments/network-chaos/network-partition.yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: partition-payment
  namespace: ecommerce
spec:
  action: partition
  mode: all
  selector:
    namespaces:
      - ecommerce
    labelSelectors:
      app: order-service
  direction: both
  target:
    selector:
      namespaces:
        - ecommerce
      labelSelectors:
        app: payment-service
    mode: all
  duration: "120s"

---
# experiments/network-chaos/network-loss.yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: packet-loss
  namespace: ecommerce
spec:
  action: loss
  mode: all
  selector:
    namespaces:
      - ecommerce
    labelSelectors:
      app: order-service
  loss:
    loss: "30"                     # 30% packet loss
    correlation: "50"
  duration: "180s"
```

### Phase 4: Stress + IO Chaos (Day 6)

```yaml
# experiments/stress-chaos/cpu-stress.yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: cpu-stress-order
  namespace: ecommerce
spec:
  mode: one
  selector:
    namespaces:
      - ecommerce
    labelSelectors:
      app: order-service
  stressors:
    cpu:
      workers: 2
      load: 80                     # 80% CPU load
  duration: "300s"

---
# experiments/io-chaos/io-delay.yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: IOChaos
metadata:
  name: io-delay-postgres
  namespace: ecommerce
spec:
  action: latency
  mode: one
  selector:
    namespaces:
      - ecommerce
    labelSelectors:
      app: postgres
  volumePath: /var/lib/postgresql/data
  path: "*"
  delay: "100ms"
  percent: 50
  duration: "300s"
```

### Phase 5: Define SLOs (Day 7)

```yaml
# slos/availability-slo.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: slo-availability
  namespace: observability
spec:
  groups:
    - name: slo.availability
      rules:
        # SLO: 99.9% availability (43.8 min downtime/month)
        - record: slo:order_service:availability:rate5m
          expr: |
            1 - (
              sum(rate(http_server_request_count{service_name="order-service",http_status_code=~"5.."}[5m]))
              /
              sum(rate(http_server_request_count{service_name="order-service"}[5m]))
            )

        - alert: SLOAvailabilityBreach
          expr: slo:order_service:availability:rate5m < 0.999
          for: 5m
          labels:
            severity: critical
            slo: availability
          annotations:
            summary: "Order service availability below 99.9% SLO"
            current: "{{ $value | humanizePercentage }}"

---
# slos/latency-slo.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: slo-latency
  namespace: observability
spec:
  groups:
    - name: slo.latency
      rules:
        # SLO: p99 latency < 500ms
        - record: slo:order_service:latency_p99:5m
          expr: |
            histogram_quantile(0.99,
              sum(rate(http_server_duration_bucket{service_name="order-service"}[5m])) by (le)
            )

        - alert: SLOLatencyBreach
          expr: slo:order_service:latency_p99:5m > 500
          for: 5m
          labels:
            severity: warning
            slo: latency
```

### Phase 6: GameDay (Day 8-12)

```markdown
# gamedays/gameday-1-pod-resilience/hypothesis.md

## GameDay 1: Pod Resilience

### Hypothesis
When we kill 50% of order-service pods during normal load (100 req/s),
the system should:
1. Maintain 99.9% availability (< 0.1% error rate)
2. Keep p99 latency under 1000ms (relaxed from 500ms SLO during incident)
3. Self-heal within 60 seconds (K8s restarts killed pods)
4. No data loss (all orders placed during chaos are persisted)

### Steady State
- order-service: 3 replicas, handling 100 req/s
- Error rate: < 0.01%
- p99 latency: ~200ms

### Experiment
1. Start load test (100 req/s sustained)
2. Wait 2 minutes for steady state
3. Kill 50% of order-service pods
4. Observe for 5 minutes
5. Verify all metrics return to steady state
6. Verify no orders were lost

### Abort Conditions
- Error rate exceeds 5% for more than 30 seconds
- Any 503 response from the database layer
- Cascading failures detected in payment-service
```

```javascript
// gamedays/gameday-1-pod-resilience/load-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const orderErrors = new Counter('order_errors');
const orderLatency = new Trend('order_latency');

export const options = {
  scenarios: {
    steady_load: {
      executor: 'constant-arrival-rate',
      rate: 100,               // 100 requests per second
      timeUnit: '1s',
      duration: '10m',         // Run for 10 minutes
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],     // SLO: 99.9% success
    order_latency: ['p(99)<1000'],       // SLO: p99 < 1s during chaos
  },
};

export default function () {
  // Mix of operations
  const ops = [
    { weight: 50, fn: browseProducts },
    { weight: 30, fn: getOrder },
    { weight: 20, fn: createOrder },
  ];

  const r = Math.random() * 100;
  let cumulative = 0;
  for (const op of ops) {
    cumulative += op.weight;
    if (r < cumulative) {
      op.fn();
      break;
    }
  }
}

function createOrder() {
  const start = Date.now();
  const res = http.post('http://localhost:8080/api/v1/orders', JSON.stringify({
    user_id: `user-${Math.floor(Math.random() * 1000)}`,
    items: [{ product_id: 'prod-1', quantity: 1 }],
  }), { headers: { 'Content-Type': 'application/json' } });

  orderLatency.add(Date.now() - start);

  const ok = check(res, {
    'order created': (r) => r.status === 200 || r.status === 201 || r.status === 202,
  });
  if (!ok) orderErrors.add(1);
}

function browseProducts() {
  http.get('http://localhost:8080/api/v1/products');
}

function getOrder() {
  http.get('http://localhost:8080/api/v1/orders/latest');
}
```

**Run the GameDay:**
```bash
#!/bin/bash
# gamedays/gameday-1-pod-resilience/run.sh

echo "=== GameDay 1: Pod Resilience ==="
echo "Starting load test..."
k6 run load-test.js &
LOAD_PID=$!

echo "Waiting 2 minutes for steady state..."
sleep 120

echo "Injecting chaos: killing 50% of order-service pods"
kubectl apply -f experiment.yaml

echo "Chaos active. Observing for 5 minutes..."
echo "Watch Grafana: http://localhost:3000/d/slo-dashboard"
sleep 300

echo "Removing chaos..."
kubectl delete -f experiment.yaml

echo "Waiting for recovery..."
sleep 120

echo "Stopping load test..."
kill $LOAD_PID

echo "=== GameDay Complete ==="
echo "Check results in Grafana and k6 output"
```

---

## Experiment Progression

| Week | Experiment | Blast Radius | Expected Outcome |
|------|-----------|-------------|-----------------|
| 1 | Kill 1 pod | Single pod | Seamless failover |
| 2 | Kill 50% pods | Service-level | Brief latency spike, self-heal |
| 3 | Network delay 200ms | Service-to-DB | Circuit breaker activates |
| 4 | Network partition | Service-to-service | Saga compensation runs |
| 5 | CPU stress 80% | Single pod | HPA scales up |
| 6 | Kafka broker kill | Data plane | Producers buffer, consumers resume |
| 7 | Combined: pod kill + network delay | Multi-failure | Full resilience validation |
| 8 | GameDay: all above during peak load | System-wide | SLOs maintained |

---

## Validation Checklist

- [ ] Chaos Mesh installed and dashboard accessible
- [ ] Pod kill: pods restart within 30 seconds, traffic redirects to healthy pods
- [ ] Network delay: circuit breakers activate, latency increases gracefully
- [ ] Network partition: saga compensations execute correctly
- [ ] CPU stress: HPA/KEDA scales up within configured thresholds
- [ ] IO delay: database queries slow but don't timeout
- [ ] Kafka broker kill: producers buffer, consumers resume from last offset
- [ ] SLO alerts fire during chaos (as expected)
- [ ] SLOs recover after chaos ends
- [ ] No data loss during any experiment
- [ ] GameDay completed: hypothesis validated or disproved
- [ ] Runbooks updated with learnings

---

## Key Concepts to Internalize

1. **Steady-state hypothesis**: define "normal" before injecting chaos. Measure deviation.
2. **Blast radius**: start small (1 pod), increase gradually. Never start with system-wide chaos.
3. **Abort conditions**: always have a kill switch. Know when to stop the experiment.
4. **Chaos is not breaking things for fun**: it's building confidence through evidence.
5. **SLOs are the success criteria**: if SLOs hold during chaos, your system is resilient.
6. **Fix before you chaos again**: found a weakness? Fix it, then re-run the experiment.
