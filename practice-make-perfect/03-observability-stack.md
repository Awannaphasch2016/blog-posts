# Project 03: Full Observability Stack for Chat Platform
# TIER 1 -- Foundation

## Goal
Instrument every chat service with OpenTelemetry and deploy a complete observability
stack: distributed tracing for chat flows (Jaeger), real-time metrics (Prometheus),
AI latency monitoring, and centralized logging (Loki) for conversation analysis.

---

## Architecture

```
  [Chat Services with OTel SDK]
  (user, chat, ai, history, websocket-gateway)
                 |
      [OpenTelemetry Collector]
         /       |       \
    [Jaeger]  [Prometheus]  [Loki]
   (chat traces) (AI latency) (conversation logs)
         \       |       /
         [Grafana Dashboards]
        (Chat Metrics, AI Performance)
                 |
         [Alertmanager]
      (AI downtime, high latency)
```

---

## Directory Structure

```
03-observability/
├── otel/
│   ├── collector-config.yaml       # OTel Collector pipeline
│   ├── collector-deployment.yaml
│   └── collector-service.yaml
├── tracing/
│   ├── jaeger-values.yaml          # Helm values
│   └── jaeger-all-in-one.yaml      # Simple deployment
├── metrics/
│   ├── prometheus-values.yaml
│   ├── service-monitors/
│   │   ├── user-service-monitor.yaml
│   │   ├── product-service-monitor.yaml
│   │   └── order-service-monitor.yaml
│   └── alerting-rules/
│       ├── high-error-rate.yaml
│       ├── high-latency.yaml
│       └── pod-restart.yaml
├── logging/
│   ├── loki-values.yaml
│   └── promtail-values.yaml
├── dashboards/
│   ├── grafana-values.yaml
│   ├── service-overview.json       # RED metrics dashboard
│   ├── kubernetes-cluster.json
│   └── kafka-dashboard.json        # For later use
├── instrumentation/
│   ├── go/                         # OTel setup for Go services
│   │   ├── otel.go
│   │   └── middleware.go
│   ├── typescript/                 # OTel setup for TS services
│   │   ├── tracing.ts
│   │   └── middleware.ts
│   └── python/                     # OTel setup for Python services
│       ├── tracing.py
│       └── middleware.py
└── k8s/
    └── namespace.yaml
```

---

## Step-by-Step Implementation

### Phase 1: Deploy Observability Infrastructure (Day 1-3)

```bash
# Create namespace
kubectl create namespace observability

# Install Prometheus + Grafana via kube-prometheus-stack
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus prometheus-community/kube-prometheus-stack \
  -n observability \
  -f metrics/prometheus-values.yaml

# Install Jaeger
helm repo add jaegertracing https://jaegertracing.github.io/helm-charts
helm install jaeger jaegertracing/jaeger \
  -n observability \
  --set allInOne.enabled=true \
  --set query.enabled=false \
  --set collector.enabled=false \
  --set agent.enabled=false

# Install Loki + Promtail
helm repo add grafana https://grafana.github.io/helm-charts
helm install loki grafana/loki-stack \
  -n observability \
  --set promtail.enabled=true \
  --set loki.persistence.enabled=false
```

**Prometheus values (enable ServiceMonitor discovery):**
```yaml
# metrics/prometheus-values.yaml
prometheus:
  prometheusSpec:
    serviceMonitorSelectorNilUsesHelmValues: false
    serviceMonitorSelector: {}
    serviceMonitorNamespaceSelector: {}
    scrapeInterval: 15s
    evaluationInterval: 15s
    retention: 7d
    storageSpec:
      volumeClaimTemplate:
        spec:
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: 5Gi

grafana:
  enabled: true
  adminPassword: admin
  additionalDataSources:
    - name: Jaeger
      type: jaeger
      url: http://jaeger-query.observability:16686
      access: proxy
    - name: Loki
      type: loki
      url: http://loki.observability:3100
      access: proxy

alertmanager:
  enabled: true
```

### Phase 2: OpenTelemetry Collector (Day 4-5)

```yaml
# otel/collector-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-collector-config
  namespace: observability
data:
  config.yaml: |
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317
          http:
            endpoint: 0.0.0.0:4318

    processors:
      batch:
        timeout: 5s
        send_batch_size: 1024
      memory_limiter:
        check_interval: 1s
        limit_mib: 512
        spike_limit_mib: 128
      resource:
        attributes:
          - key: environment
            value: local
            action: upsert

    exporters:
      otlp/jaeger:
        endpoint: jaeger-collector.observability:4317
        tls:
          insecure: true
      prometheus:
        endpoint: 0.0.0.0:8889
        namespace: otel
      loki:
        endpoint: http://loki.observability:3100/loki/api/v1/push
        labels:
          attributes:
            service.name: "service_name"
            service.namespace: "service_namespace"

    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [memory_limiter, batch, resource]
          exporters: [otlp/jaeger]
        metrics:
          receivers: [otlp]
          processors: [memory_limiter, batch]
          exporters: [prometheus]
        logs:
          receivers: [otlp]
          processors: [memory_limiter, batch]
          exporters: [loki]
```

```yaml
# otel/collector-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-collector
  namespace: observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app: otel-collector
  template:
    metadata:
      labels:
        app: otel-collector
    spec:
      containers:
        - name: otel-collector
          image: otel/opentelemetry-collector-contrib:latest
          args: ["--config=/etc/otel/config.yaml"]
          ports:
            - containerPort: 4317    # OTLP gRPC
            - containerPort: 4318    # OTLP HTTP
            - containerPort: 8889    # Prometheus exporter
          volumeMounts:
            - name: config
              mountPath: /etc/otel
          resources:
            requests:
              cpu: 200m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
      volumes:
        - name: config
          configMap:
            name: otel-collector-config
---
apiVersion: v1
kind: Service
metadata:
  name: otel-collector
  namespace: observability
spec:
  selector:
    app: otel-collector
  ports:
    - name: otlp-grpc
      port: 4317
      targetPort: 4317
    - name: otlp-http
      port: 4318
      targetPort: 4318
    - name: prometheus
      port: 8889
      targetPort: 8889
```

### Phase 3: Instrument Services (Day 6-10)

**Go instrumentation (user-service, order-service, shipping-service):**
```go
// instrumentation/go/otel.go
package otel

import (
    "context"
    "os"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/metric"
    "go.opentelemetry.io/otel/sdk/resource"
    "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func InitTelemetry(ctx context.Context, serviceName string) (func(), error) {
    res, _ := resource.New(ctx,
        resource.WithAttributes(
            semconv.ServiceNameKey.String(serviceName),
            semconv.ServiceVersionKey.String(os.Getenv("VERSION")),
            semconv.DeploymentEnvironmentKey.String("local"),
        ),
    )

    // Trace exporter
    traceExporter, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint("otel-collector.observability:4317"),
        otlptracegrpc.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }

    tp := trace.NewTracerProvider(
        trace.WithBatcher(traceExporter),
        trace.WithResource(res),
        trace.WithSampler(trace.AlwaysSample()),
    )
    otel.SetTracerProvider(tp)

    // Metric exporter
    metricExporter, err := otlpmetricgrpc.New(ctx,
        otlpmetricgrpc.WithEndpoint("otel-collector.observability:4317"),
        otlpmetricgrpc.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }

    mp := metric.NewMeterProvider(
        metric.WithReader(metric.NewPeriodicReader(metricExporter)),
        metric.WithResource(res),
    )
    otel.SetMeterProvider(mp)

    // Propagation
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))

    return func() {
        tp.Shutdown(ctx)
        mp.Shutdown(ctx)
    }, nil
}
```

```go
// instrumentation/go/middleware.go
package otel

import (
    "net/http"

    "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    otelmetric "go.opentelemetry.io/otel/metric"
)

var (
    requestCounter  otelmetric.Int64Counter
    requestDuration otelmetric.Float64Histogram
)

func InitMetrics(serviceName string) {
    meter := otel.Meter(serviceName)
    requestCounter, _ = meter.Int64Counter("http.server.request_count",
        otelmetric.WithDescription("Total HTTP requests"),
    )
    requestDuration, _ = meter.Float64Histogram("http.server.duration",
        otelmetric.WithDescription("HTTP request duration in ms"),
        otelmetric.WithUnit("ms"),
    )
}

// Wrap your HTTP handler for automatic tracing
func InstrumentHandler(pattern string, handler http.Handler) http.Handler {
    return otelhttp.NewHandler(handler, pattern,
        otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
    )
}

// Custom middleware for RED metrics
func REDMetricsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // The otelhttp handler already tracks these, but you can add custom attributes
        attrs := []attribute.KeyValue{
            attribute.String("http.method", r.Method),
            attribute.String("http.route", r.URL.Path),
        }
        requestCounter.Add(r.Context(), 1, otelmetric.WithAttributes(attrs...))
        next.ServeHTTP(w, r)
    })
}
```

**TypeScript instrumentation (product-service, payment-service):**
```typescript
// instrumentation/typescript/tracing.ts
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';

const resource = new Resource({
  [ATTR_SERVICE_NAME]: process.env.SERVICE_NAME || 'unknown',
  [ATTR_SERVICE_VERSION]: process.env.VERSION || '0.0.1',
  'deployment.environment': 'local',
});

const sdk = new NodeSDK({
  resource,
  traceExporter: new OTLPTraceExporter({
    url: 'grpc://otel-collector.observability:4317',
  }),
  metricReader: new PeriodicExportingMetricReader({
    exporter: new OTLPMetricExporter({
      url: 'grpc://otel-collector.observability:4317',
    }),
    exportIntervalMillis: 15000,
  }),
  instrumentations: [
    getNodeAutoInstrumentations({
      '@opentelemetry/instrumentation-http': { enabled: true },
      '@opentelemetry/instrumentation-express': { enabled: true },
      '@opentelemetry/instrumentation-grpc': { enabled: true },
      '@opentelemetry/instrumentation-mongodb': { enabled: true },
    }),
  ],
});

sdk.start();
process.on('SIGTERM', () => sdk.shutdown());
```

**Python instrumentation (cart-service, notification-service):**
```python
# instrumentation/python/tracing.py
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
import os


def init_telemetry(service_name: str):
    resource = Resource.create({
        SERVICE_NAME: service_name,
        SERVICE_VERSION: os.getenv("VERSION", "0.0.1"),
        "deployment.environment": "local",
    })

    # Traces
    trace_exporter = OTLPSpanExporter(
        endpoint="otel-collector.observability:4317",
        insecure=True,
    )
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(trace_exporter))
    trace.set_tracer_provider(tp)

    # Metrics
    metric_exporter = OTLPMetricExporter(
        endpoint="otel-collector.observability:4317",
        insecure=True,
    )
    reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=15000)
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)

    # Auto-instrument
    FastAPIInstrumentor.instrument()
    RedisInstrumentor().instrument()
```

### Phase 4: ServiceMonitors for Prometheus (Day 11)

```yaml
# metrics/service-monitors/user-service-monitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: user-service
  namespace: ecommerce
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: user-service
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
  namespaceSelector:
    matchNames:
      - ecommerce
```

### Phase 5: Alerting Rules (Day 12)

```yaml
# metrics/alerting-rules/high-error-rate.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: service-alerts
  namespace: observability
  labels:
    release: prometheus
spec:
  groups:
    - name: service.rules
      rules:
        - alert: HighErrorRate
          expr: |
            (
              sum(rate(http_server_request_count{http_status_code=~"5.."}[5m])) by (service_name)
              /
              sum(rate(http_server_request_count[5m])) by (service_name)
            ) > 0.05
          for: 2m
          labels:
            severity: critical
          annotations:
            summary: "High error rate on {{ $labels.service_name }}"
            description: "Error rate is {{ $value | humanizePercentage }} (threshold: 5%)"

        - alert: HighLatency
          expr: |
            histogram_quantile(0.99,
              sum(rate(http_server_duration_bucket[5m])) by (le, service_name)
            ) > 2000
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High p99 latency on {{ $labels.service_name }}"
            description: "p99 latency is {{ $value }}ms (threshold: 2000ms)"

        - alert: PodCrashLooping
          expr: rate(kube_pod_container_status_restarts_total[15m]) > 0
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Pod {{ $labels.pod }} is crash looping"
```

### Phase 6: Grafana Dashboard (Day 13-14)

Create a RED metrics dashboard JSON (import into Grafana):

**Key panels to build:**
1. **Request Rate** -- `sum(rate(http_server_request_count[5m])) by (service_name)`
2. **Error Rate %** -- 5xx / total requests per service
3. **Latency p50/p95/p99** -- `histogram_quantile(0.99, ...)`
4. **Trace search** -- linked to Jaeger datasource
5. **Log stream** -- linked to Loki datasource with service filter
6. **Pod status** -- Running/Pending/Failed per namespace
7. **Resource usage** -- CPU/memory per service from cAdvisor metrics

---

## Validation Checklist

- [ ] OTel Collector receiving traces, metrics, and logs
- [ ] Jaeger UI shows traces spanning multiple services (e.g., BFF -> order -> user -> payment)
- [ ] Trace IDs propagated across service boundaries (check headers)
- [ ] Prometheus scraping all ServiceMonitors
- [ ] Grafana dashboards showing RED metrics per service
- [ ] Loki receiving structured logs from all services
- [ ] Can correlate: trace -> logs -> metrics for a single request
- [ ] Alerting rules firing correctly (test by crashing a service)
- [ ] `kubectl port-forward svc/grafana 3000:80 -n observability` works

---

## Key Concepts to Internalize

1. **Three pillars**: traces (request flow), metrics (aggregated numbers), logs (discrete events)
2. **RED method**: Rate, Errors, Duration -- the 3 metrics every service needs
3. **Trace context propagation**: W3C TraceContext headers must flow through every hop
4. **OTel Collector as pipeline**: receivers -> processors -> exporters (decouples apps from backends)
5. **ServiceMonitor**: Prometheus Operator CRD that auto-discovers scrape targets
6. **Exemplars**: link metrics to traces (click a latency spike, jump to the exact trace)
