# Project 10: Real-time Chat Analytics with Debezium CDC
# TIER 3 -- Expert Platform Engineering

## Goal
Implement real-time data synchronization between microservices using Debezium
for Change Data Capture (CDC). Implement the Outbox Pattern to solve the
dual-write problem.

---

## Architecture

```
  [order-service]
       |
  INSERT order + INSERT outbox_event (single transaction)
       |
  [PostgreSQL]
       |
  [Debezium Connector] (reads WAL)
       |
  [Kafka]
       |
  +----+----+
  |         |
[ES projection]  [Analytics pipeline]
  |
[Elasticsearch]
```

---

## Directory Structure

```
10-cdc-debezium/
├── debezium/
│   ├── kafka-connect-cluster.yaml  # Strimzi KafkaConnect
│   ├── connectors/
│   │   ├── order-db-connector.yaml
│   │   ├── user-db-connector.yaml
│   │   └── outbox-connector.yaml
│   └── transforms/
│       └── outbox-route-transform.json
├── database/
│   ├── migrations/
│   │   ├── 001_create_orders.sql
│   │   ├── 002_create_outbox.sql
│   │   └── 003_enable_wal.sql
│   └── outbox-table.sql
├── services/
│   ├── order-service/
│   │   └── outbox_publisher.go     # Write to outbox table
│   └── projections/
│       ├── es-sync/                # Sync to Elasticsearch
│       │   └── main.go
│       └── redis-cache/            # Sync to Redis
│           └── main.go
├── k8s/
│   └── kafka-connect/
│       └── deployment.yaml
└── monitoring/
    └── debezium-dashboard.json
```

---

## Step-by-Step Implementation

### Phase 1: PostgreSQL WAL Configuration (Day 1)

```sql
-- database/migrations/003_enable_wal.sql
-- PostgreSQL must have logical replication enabled
-- In postgresql.conf (or via K8s ConfigMap):
--   wal_level = logical
--   max_wal_senders = 4
--   max_replication_slots = 4

-- Verify:
SHOW wal_level;  -- Should be 'logical'
```

```yaml
# Update postgres StatefulSet to enable logical replication
containers:
  - name: postgres
    image: postgres:16-alpine
    args:
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_wal_senders=4"
      - "-c"
      - "max_replication_slots=4"
```

### Phase 2: Outbox Table (Day 2)

```sql
-- database/outbox-table.sql
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(255) NOT NULL,    -- e.g., "Order"
    aggregate_id    VARCHAR(255) NOT NULL,    -- e.g., order UUID
    event_type      VARCHAR(255) NOT NULL,    -- e.g., "OrderCreated"
    payload         JSONB NOT NULL,           -- Event data
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Debezium reads and deletes processed rows (optional)
    -- Or use a log-compacted Kafka topic
    metadata        JSONB                     -- trace_id, correlation_id, etc.
);

-- Index for Debezium connector performance
CREATE INDEX idx_outbox_created_at ON outbox_events(created_at);
```

### Phase 3: Outbox Publisher in Service (Day 3-4)

```go
// services/order-service/outbox_publisher.go
package order

import (
    "context"
    "database/sql"
    "encoding/json"
)

type OutboxEvent struct {
    AggregateType string `json:"aggregate_type"`
    AggregateID   string `json:"aggregate_id"`
    EventType     string `json:"event_type"`
    Payload       any    `json:"payload"`
    Metadata      any    `json:"metadata"`
}

type OrderRepository struct {
    db *sql.DB
}

// CreateOrder writes the order AND the outbox event in a SINGLE transaction.
// This is the key insight: no dual-write problem because it's one atomic operation.
func (r *OrderRepository) CreateOrder(ctx context.Context, order *Order) error {
    tx, err := r.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // 1. Insert the order
    _, err = tx.ExecContext(ctx, `
        INSERT INTO orders (id, user_id, status, total_cents, created_at)
        VALUES ($1, $2, $3, $4, NOW())`,
        order.ID, order.UserID, "PENDING", order.TotalCents,
    )
    if err != nil {
        return err
    }

    // 2. Insert items
    for _, item := range order.Items {
        _, err = tx.ExecContext(ctx, `
            INSERT INTO order_items (order_id, product_id, quantity, price_cents)
            VALUES ($1, $2, $3, $4)`,
            order.ID, item.ProductID, item.Quantity, item.PriceCents,
        )
        if err != nil {
            return err
        }
    }

    // 3. Insert outbox event (same transaction!)
    payload, _ := json.Marshal(OrderCreatedPayload{
        OrderID:    order.ID,
        UserID:     order.UserID,
        Items:      order.Items,
        TotalCents: order.TotalCents,
    })

    metadata, _ := json.Marshal(map[string]string{
        "trace_id":      getTraceID(ctx),
        "correlation_id": getCorrelationID(ctx),
    })

    _, err = tx.ExecContext(ctx, `
        INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, metadata)
        VALUES ($1, $2, $3, $4, $5)`,
        "Order", order.ID, "OrderCreated", payload, metadata,
    )
    if err != nil {
        return err
    }

    // Both writes succeed or both fail. No inconsistency possible.
    return tx.Commit()
}
```

### Phase 4: Deploy Kafka Connect + Debezium (Day 5-7)

```yaml
# debezium/kafka-connect-cluster.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: debezium-connect
  namespace: kafka
  annotations:
    strimzi.io/use-connector-resources: "true"
spec:
  version: 3.7.0
  replicas: 1
  bootstrapServers: ecommerce-cluster-kafka-bootstrap:9092
  config:
    group.id: debezium-connect
    offset.storage.topic: connect-offsets
    config.storage.topic: connect-configs
    status.storage.topic: connect-status
    offset.storage.replication.factor: 3
    config.storage.replication.factor: 3
    status.storage.replication.factor: 3
  build:
    output:
      type: docker
      image: debezium-connect:latest
    plugins:
      - name: debezium-postgres
        artifacts:
          - type: maven
            group: io.debezium
            artifact: debezium-connector-postgres
            version: 2.5.0.Final
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: 500m
      memory: 1Gi
```

```yaml
# debezium/connectors/outbox-connector.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: order-outbox-connector
  namespace: kafka
  labels:
    strimzi.io/cluster: debezium-connect
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  config:
    # Database connection
    database.hostname: postgres.ecommerce
    database.port: 5432
    database.user: debezium
    database.password: ${file:/opt/kafka/external-configuration/db-credentials/password}
    database.dbname: orderdb
    topic.prefix: order-db

    # Outbox pattern: only capture the outbox table
    table.include.list: public.outbox_events

    # Outbox Event Router SMT (Single Message Transform)
    transforms: outbox
    transforms.outbox.type: io.debezium.transforms.outbox.EventRouter
    transforms.outbox.table.field.event.id: id
    transforms.outbox.table.field.event.key: aggregate_id
    transforms.outbox.table.field.event.type: event_type
    transforms.outbox.table.field.event.payload: payload
    transforms.outbox.table.fields.additional.placement: metadata:header
    transforms.outbox.route.by.field: aggregate_type
    transforms.outbox.route.topic.replacement: events.${routedByValue}

    # Result: outbox_events rows become Kafka messages on topic "events.Order"
    # Key = aggregate_id (order UUID)
    # Value = payload JSON
    # Headers include metadata (trace_id, correlation_id)

    # Cleanup: delete outbox rows after capture
    transforms.outbox.table.expand.json.payload: true

    # Slot and publication
    slot.name: outbox_slot
    publication.name: outbox_publication
    plugin.name: pgoutput

    # Heartbeat to keep replication slot active
    heartbeat.interval.ms: 60000
```

### Phase 5: CDC Projection Service (Day 8-9)

```go
// services/projections/es-sync/main.go
package main

// Consumes from "events.Order" topic (produced by Debezium outbox router)
// and syncs order data to Elasticsearch for search

import (
    "context"
    "encoding/json"
    "log"

    "github.com/segmentio/kafka-go"
    "github.com/elastic/go-elasticsearch/v8"
)

func main() {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"ecommerce-cluster-kafka-bootstrap.kafka:9092"},
        Topic:   "events.Order",    // Debezium routed topic
        GroupID: "es-sync",
    })

    es, _ := elasticsearch.NewClient(elasticsearch.Config{
        Addresses: []string{"http://elasticsearch.observability:9200"},
    })

    for {
        msg, err := reader.FetchMessage(context.Background())
        if err != nil {
            log.Printf("fetch: %v", err)
            continue
        }

        eventType := getHeader(msg.Headers, "event_type")

        switch eventType {
        case "OrderCreated":
            syncOrderCreated(es, msg.Key, msg.Value)
        case "OrderConfirmed":
            updateOrderStatus(es, msg.Key, "CONFIRMED")
        case "OrderCancelled":
            updateOrderStatus(es, msg.Key, "CANCELLED")
        }

        reader.CommitMessages(context.Background(), msg)
    }
}

func syncOrderCreated(es *elasticsearch.Client, key, value []byte) {
    var payload map[string]any
    json.Unmarshal(value, &payload)

    body, _ := json.Marshal(payload)
    es.Index("orders",
        bytes.NewReader(body),
        es.Index.WithDocumentID(string(key)),
    )
    log.Printf("synced order %s to ES", string(key))
}
```

### Phase 6: Full CDC (Direct Table Capture) (Day 10-11)

```yaml
# debezium/connectors/order-db-connector.yaml
# Capture ALL changes from the orders database (not just outbox)
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: order-db-cdc-connector
  namespace: kafka
  labels:
    strimzi.io/cluster: debezium-connect
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  config:
    database.hostname: postgres.ecommerce
    database.port: 5432
    database.user: debezium
    database.password: ${file:/opt/kafka/external-configuration/db-credentials/password}
    database.dbname: orderdb
    topic.prefix: cdc.orderdb

    # Capture orders and order_items tables (not outbox)
    table.include.list: public.orders,public.order_items

    # Each table gets its own topic:
    #   cdc.orderdb.public.orders
    #   cdc.orderdb.public.order_items

    # Message format: includes before/after state + operation type
    # op: "c" = create, "u" = update, "d" = delete, "r" = read (snapshot)

    slot.name: orderdb_slot
    plugin.name: pgoutput

    # Include transaction info
    provide.transaction.metadata: true

    # Snapshot mode: capture existing data first, then stream changes
    snapshot.mode: initial
```

---

## Outbox Pattern vs Direct CDC

| Aspect | Outbox Pattern | Direct Table CDC |
|--------|---------------|-----------------|
| **What's captured** | Domain events (explicit) | All row changes (implicit) |
| **Schema control** | You define the event schema | Mirrors DB schema |
| **Dual-write safety** | Guaranteed (single transaction) | N/A (DB is source of truth) |
| **Use case** | Publishing domain events | Data replication, analytics |
| **Consumer complexity** | Simple (clean events) | Higher (DB schema knowledge needed) |

**Use Outbox for**: domain events across microservices
**Use Direct CDC for**: data lake ingestion, analytics, read replica sync

---

## Validation Checklist

- [ ] PostgreSQL WAL level set to `logical`
- [ ] Outbox table created with proper schema
- [ ] Order creation writes order + outbox event in single transaction
- [ ] Kafka Connect cluster running with Debezium plugin
- [ ] Outbox connector captures outbox_events and routes to topic
- [ ] Events appear on `events.Order` topic with correct key/value
- [ ] trace_id and correlation_id propagated in Kafka headers
- [ ] ES-sync consumer builds search index from CDC events
- [ ] Direct CDC connector captures all orders table changes
- [ ] Debezium survives connector restart (resumes from last offset)
- [ ] Monitoring: connector status, lag, replication slot size
- [ ] No dual-write: order + event always consistent

---

## Key Concepts to Internalize

1. **Dual-write problem**: writing to DB + Kafka separately = guaranteed inconsistency eventually. Outbox solves this.
2. **Outbox pattern**: write event to DB table in same transaction as business data. CDC picks it up.
3. **WAL-based CDC**: reads the write-ahead log, not polling. Zero impact on DB performance.
4. **Debezium Event Router**: transforms outbox table rows into properly routed Kafka messages.
5. **Snapshot + streaming**: Debezium captures existing data first, then streams real-time changes.
6. **Replication slots**: PostgreSQL reserves WAL segments until Debezium confirms. Monitor slot lag!
