# Project 04: Event-Driven Chat Architecture with Kafka
# TIER 2 -- Production Patterns

## Goal
Implement real-time chat messaging with asynchronous event-driven architecture
using Kafka (deployed via Strimzi Operator), with message ordering, chat event
streaming, dead-letter queues, and KEDA-based consumer autoscaling.

---

## Architecture

```
  [chat-service]
       |
  MessageSent event
       |
  [Kafka (Strimzi)] -----> [Schema Registry]
       |
  +----+----+----+----+
  |         |         |         |
[ai-service] [history] [analytics] [moderation]
  |         |         |         |
AIResponse  MessageStored  UserActivity  ContentChecked
  |         |         |
  +----+----+----+----+
       |
  [websocket-gateway]
       |
  RealTimeNotification
```

---

## Directory Structure

```
04-event-driven-kafka/
├── strimzi/
│   ├── strimzi-operator.yaml       # Or install via Helm
│   ├── kafka-cluster.yaml          # 3-broker cluster
│   ├── kafka-topics/
│   │   ├── order-events.yaml
│   │   ├── payment-events.yaml
│   │   ├── inventory-events.yaml
│   │   ├── shipping-events.yaml
│   │   ├── notification-events.yaml
│   │   └── dead-letter.yaml
│   └── kafka-users/
│       ├── order-service-user.yaml
│       ├── payment-service-user.yaml
│       └── inventory-service-user.yaml
├── schema-registry/
│   ├── deployment.yaml
│   ├── schemas/
│   │   ├── order-created.avsc
│   │   ├── payment-processed.avsc
│   │   ├── stock-reserved.avsc
│   │   └── shipment-dispatched.avsc
│   └── compatibility-config.yaml
├── services/
│   ├── order-service/
│   │   └── producer.go             # Publishes OrderCreated
│   ├── inventory-service/
│   │   ├── consumer.go             # Consumes OrderCreated
│   │   └── producer.go             # Publishes StockReserved
│   ├── payment-service/
│   │   ├── consumer.ts             # Consumes StockReserved
│   │   └── producer.ts             # Publishes PaymentProcessed
│   ├── notification-service/
│   │   └── consumer.py             # Consumes all events
│   └── shipping-service/
│       └── consumer.go             # Consumes PaymentProcessed
├── keda/
│   ├── keda-install.yaml
│   ├── scaled-objects/
│   │   ├── inventory-scaler.yaml
│   │   ├── payment-scaler.yaml
│   │   └── notification-scaler.yaml
│   └── trigger-auth.yaml
└── monitoring/
    └── kafka-dashboard.json
```

---

## Step-by-Step Implementation

### Phase 1: Deploy Strimzi + Kafka Cluster (Day 1-3)

```bash
# Install Strimzi operator
kubectl create namespace kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Wait for operator to be ready
kubectl wait deployment/strimzi-cluster-operator -n kafka --for=condition=Available --timeout=300s
```

```yaml
# strimzi/kafka-cluster.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: ecommerce-cluster
  namespace: kafka
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
      auto.create.topics.enable: false
    storage:
      type: jbod
      volumes:
        - id: 0
          type: persistent-claim
          size: 5Gi
          deleteClaim: true
    resources:
      requests:
        memory: 512Mi
        cpu: 250m
      limits:
        memory: 1Gi
        cpu: 500m
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 2Gi
      deleteClaim: true
    resources:
      requests:
        memory: 256Mi
        cpu: 100m
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

### Phase 2: Create Topics via CRDs (Day 3)

```yaml
# strimzi/kafka-topics/order-events.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: order.events
  namespace: kafka
  labels:
    strimzi.io/cluster: ecommerce-cluster
spec:
  partitions: 6
  replicas: 3
  config:
    retention.ms: 604800000     # 7 days
    cleanup.policy: delete
    min.insync.replicas: 2
    compression.type: lz4
---
# strimzi/kafka-topics/dead-letter.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: dead-letter
  namespace: kafka
  labels:
    strimzi.io/cluster: ecommerce-cluster
spec:
  partitions: 3
  replicas: 3
  config:
    retention.ms: 2592000000    # 30 days
    cleanup.policy: delete
```

### Phase 3: Avro Schemas + Schema Registry (Day 4-5)

```bash
# Deploy Confluent Schema Registry (or Apicurio)
helm repo add confluentinc https://packages.confluent.io/helm
helm install schema-registry confluentinc/cp-schema-registry \
  -n kafka \
  --set kafka.bootstrapServers="ecommerce-cluster-kafka-bootstrap.kafka:9092"
```

```json
// schema-registry/schemas/order-created.avsc
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.ecommerce.events.order",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "order_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderItem",
          "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "quantity", "type": "int"},
            {"name": "price_cents", "type": "long"}
          ]
        }
      }
    },
    {"name": "total_cents", "type": "long"},
    {"name": "currency", "type": "string", "default": "USD"}
  ]
}
```

### Phase 4: Producer Implementation (Day 6-7)

```go
// services/order-service/producer.go
package kafka

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/segmentio/kafka-go"
    "github.com/google/uuid"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
)

type OrderCreatedEvent struct {
    EventID        string      `json:"event_id"`
    EventTimestamp int64       `json:"event_timestamp"`
    OrderID        string      `json:"order_id"`
    UserID         string      `json:"user_id"`
    Items          []OrderItem `json:"items"`
    TotalCents     int64       `json:"total_cents"`
    Currency       string      `json:"currency"`
}

type EventProducer struct {
    writer *kafka.Writer
}

func NewEventProducer(brokers []string) *EventProducer {
    return &EventProducer{
        writer: &kafka.Writer{
            Addr:         kafka.TCP(brokers...),
            Topic:        "order.events",
            Balancer:     &kafka.Hash{}, // Partition by key (order_id)
            RequiredAcks: kafka.RequireAll,
            MaxAttempts:  3,
            BatchTimeout: 10 * time.Millisecond,
            Compression:  kafka.Lz4,
        },
    }
}

func (p *EventProducer) PublishOrderCreated(ctx context.Context, order *Order) error {
    tracer := otel.Tracer("order-service")
    ctx, span := tracer.Start(ctx, "kafka.publish.OrderCreated")
    defer span.End()

    event := OrderCreatedEvent{
        EventID:        uuid.New().String(),
        EventTimestamp: time.Now().UnixMilli(),
        OrderID:        order.ID,
        UserID:         order.UserID,
        Items:          order.Items,
        TotalCents:     order.TotalCents,
        Currency:       "USD",
    }

    value, err := json.Marshal(event)
    if err != nil {
        return fmt.Errorf("marshal event: %w", err)
    }

    // Propagate trace context in Kafka headers
    headers := make([]kafka.Header, 0)
    carrier := &KafkaHeaderCarrier{headers: &headers}
    otel.GetTextMapPropagator().Inject(ctx, carrier)

    return p.writer.WriteMessages(ctx, kafka.Message{
        Key:     []byte(order.ID), // Ensures ordering per order
        Value:   value,
        Headers: headers,
    })
}
```

### Phase 5: Consumer with Dead-Letter Queue (Day 8-10)

```go
// services/inventory-service/consumer.go
package kafka

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/segmentio/kafka-go"
    "go.opentelemetry.io/otel"
)

type EventConsumer struct {
    reader    *kafka.Reader
    dlqWriter *kafka.Writer
    handler   func(ctx context.Context, event OrderCreatedEvent) error
}

func NewEventConsumer(brokers []string, handler func(context.Context, OrderCreatedEvent) error) *EventConsumer {
    return &EventConsumer{
        reader: kafka.NewReader(kafka.ReaderConfig{
            Brokers:        brokers,
            Topic:          "order.events",
            GroupID:        "inventory-service",
            MinBytes:       1,
            MaxBytes:       10e6,
            MaxWait:        500 * time.Millisecond,
            CommitInterval: time.Second,
            StartOffset:    kafka.LastOffset,
        }),
        dlqWriter: &kafka.Writer{
            Addr:         kafka.TCP(brokers...),
            Topic:        "dead-letter",
            RequiredAcks: kafka.RequireAll,
        },
        handler: handler,
    }
}

func (c *EventConsumer) Start(ctx context.Context) {
    for {
        msg, err := c.reader.FetchMessage(ctx)
        if err != nil {
            if ctx.Err() != nil {
                return // Shutting down
            }
            log.Printf("fetch error: %v", err)
            continue
        }

        // Extract trace context from headers
        carrier := &KafkaHeaderCarrier{headers: &msg.Headers}
        parentCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

        tracer := otel.Tracer("inventory-service")
        consumeCtx, span := tracer.Start(parentCtx, "kafka.consume.OrderCreated")

        var event OrderCreatedEvent
        if err := json.Unmarshal(msg.Value, &event); err != nil {
            log.Printf("unmarshal error: %v", err)
            c.sendToDLQ(ctx, msg, err)
            span.End()
            continue
        }

        // Process with retry (idempotent handler)
        if err := c.processWithRetry(consumeCtx, event, 3); err != nil {
            log.Printf("processing failed after retries: %v", err)
            c.sendToDLQ(ctx, msg, err)
        }

        span.End()

        // Only commit after successful processing
        if err := c.reader.CommitMessages(ctx, msg); err != nil {
            log.Printf("commit error: %v", err)
        }
    }
}

func (c *EventConsumer) processWithRetry(ctx context.Context, event OrderCreatedEvent, maxRetries int) error {
    var lastErr error
    for attempt := 0; attempt <= maxRetries; attempt++ {
        if err := c.handler(ctx, event); err != nil {
            lastErr = err
            backoff := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
            time.Sleep(backoff)
            continue
        }
        return nil
    }
    return lastErr
}

func (c *EventConsumer) sendToDLQ(ctx context.Context, msg kafka.Message, processErr error) {
    headers := append(msg.Headers,
        kafka.Header{Key: "original-topic", Value: []byte(msg.Topic)},
        kafka.Header{Key: "error", Value: []byte(processErr.Error())},
        kafka.Header{Key: "failed-at", Value: []byte(time.Now().Format(time.RFC3339))},
    )

    c.dlqWriter.WriteMessages(ctx, kafka.Message{
        Key:     msg.Key,
        Value:   msg.Value,
        Headers: headers,
    })
}
```

### Phase 6: KEDA Autoscaling (Day 11-12)

```bash
# Install KEDA
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda -n keda --create-namespace
```

```yaml
# keda/scaled-objects/inventory-scaler.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: inventory-service-scaler
  namespace: ecommerce
spec:
  scaleTargetRef:
    name: inventory-service
  pollingInterval: 15
  cooldownPeriod: 60
  minReplicaCount: 1
  maxReplicaCount: 10
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: ecommerce-cluster-kafka-bootstrap.kafka:9092
        consumerGroup: inventory-service
        topic: order.events
        lagThreshold: "50"        # Scale up when lag > 50
        activationLagThreshold: "0" # Scale from 0 when any messages
        offsetResetPolicy: latest
---
# Scale-to-zero for notification service (non-critical)
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: notification-service-scaler
  namespace: ecommerce
spec:
  scaleTargetRef:
    name: notification-service
  pollingInterval: 30
  cooldownPeriod: 300
  minReplicaCount: 0              # Scale to zero!
  maxReplicaCount: 5
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: ecommerce-cluster-kafka-bootstrap.kafka:9092
        consumerGroup: notification-service
        topic: notification.events
        lagThreshold: "10"
        activationLagThreshold: "1"
```

---

## Event Flow: Complete Order Lifecycle

```
1. User places order
   -> order-service publishes OrderCreated to "order.events"

2. inventory-service consumes OrderCreated
   -> Checks stock availability
   -> Reserves stock in DB
   -> Publishes StockReserved to "inventory.events"
   (OR publishes StockInsufficient -> order-service cancels)

3. payment-service consumes StockReserved
   -> Charges payment (mock)
   -> Publishes PaymentProcessed to "payment.events"
   (OR publishes PaymentFailed -> inventory-service releases stock)

4. shipping-service consumes PaymentProcessed
   -> Creates shipment
   -> Publishes ShipmentDispatched to "shipping.events"

5. notification-service consumes ALL events
   -> Sends email/SMS at each stage (mock)

6. order-service consumes downstream events
   -> Updates order status (pending -> confirmed -> paid -> shipped)
```

---

## Validation Checklist

- [ ] Strimzi Kafka cluster running with 3 brokers
- [ ] Topics created via KafkaTopic CRDs
- [ ] Schema Registry running and schemas registered
- [ ] order-service publishes OrderCreated on checkout
- [ ] inventory-service consumes and publishes StockReserved
- [ ] payment-service consumes and publishes PaymentProcessed
- [ ] shipping-service consumes and publishes ShipmentDispatched
- [ ] notification-service consumes all event types
- [ ] Failed messages go to dead-letter topic with error metadata
- [ ] Consumer is idempotent (reprocessing same event = no side effects)
- [ ] KEDA scales consumers based on Kafka lag
- [ ] notification-service scales to zero when idle
- [ ] Traces span across Kafka producer -> consumer (via headers)
- [ ] Kafka metrics visible in Grafana (consumer lag, throughput)

---

## Key Concepts to Internalize

1. **Event ordering**: partition by order_id ensures all events for an order are ordered
2. **At-least-once delivery**: consumers must be idempotent (use event_id for dedup)
3. **Consumer groups**: each service has its own group = each gets all messages
4. **Dead-letter queues**: catch poison messages without blocking the pipeline
5. **Schema evolution**: backward-compatible changes only (add optional fields, never remove)
6. **Backpressure**: KEDA scales consumers to match producer throughput
7. **Exactly-once semantics**: use transactions or idempotent handlers, not "hope"
