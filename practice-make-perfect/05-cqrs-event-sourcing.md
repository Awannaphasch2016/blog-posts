# Project 05: CQRS + Event Sourcing for Chat History
# TIER 2 -- Production Patterns

## Goal
Implement Command Query Responsibility Segregation (CQRS) and Event Sourcing
for the Chat domain. The command side appends conversation events to an event store;
the query side builds optimized read models for conversation history, user activity,
and chat analytics via projections.

---

## Architecture

```
  [REST/gRPC Command API]        [REST/gRPC Query API]
         |                              |
  [Command Handler]              [Query Handler]
         |                              |
  [Aggregate Root]               [Read Model Store]
         |                         /         \
  [Event Store (Kafka)]     [Elasticsearch] [Redis]
         |                     (search)    (real-time)
         |
  [Projection Workers]
         |
  Build read models from event stream
```

---

## Directory Structure

```
05-cqrs-event-sourcing/
├── command-side/
│   ├── order-command-service/
│   │   ├── main.go
│   │   ├── aggregate/
│   │   │   └── order.go            # Order aggregate root
│   │   ├── commands/
│   │   │   ├── create_order.go
│   │   │   ├── confirm_order.go
│   │   │   └── cancel_order.go
│   │   ├── events/
│   │   │   ├── order_created.go
│   │   │   ├── order_confirmed.go
│   │   │   └── order_cancelled.go
│   │   ├── store/
│   │   │   ├── event_store.go      # Kafka-backed event store
│   │   │   └── snapshot_store.go   # Redis snapshots
│   │   └── handlers/
│   │       └── command_handler.go
├── query-side/
│   ├── order-query-service/
│   │   ├── main.go
│   │   ├── projections/
│   │   │   ├── order_list.go       # Elasticsearch projection
│   │   │   ├── order_summary.go    # Redis projection
│   │   │   └── revenue_stats.go    # Time-series projection
│   │   ├── handlers/
│   │   │   └── query_handler.go
│   │   └── models/
│   │       ├── order_view.go
│   │       └── revenue_view.go
├── shared/
│   ├── events/
│   │   └── event.go                # Base event interface
│   └── proto/
│       ├── command.proto
│       └── query.proto
├── k8s/
│   ├── elasticsearch/
│   │   ├── statefulset.yaml
│   │   └── service.yaml
│   ├── order-command/
│   │   ├── deployment.yaml
│   │   └── service.yaml
│   └── order-query/
│       ├── deployment.yaml
│       └── service.yaml
└── tools/
    └── replay-events.go            # Event replay utility
```

---

## Step-by-Step Implementation

### Phase 1: Event Definitions (Day 1)

```go
// shared/events/event.go
package events

import "time"

// Base event interface
type DomainEvent interface {
    EventType() string
    AggregateID() string
    AggregateType() string
    Version() int
    Timestamp() time.Time
}

type BaseEvent struct {
    ID            string    `json:"event_id"`
    Type          string    `json:"event_type"`
    AggregateId   string    `json:"aggregate_id"`
    AggregateVer  int       `json:"aggregate_version"`
    OccurredAt    time.Time `json:"occurred_at"`
    CorrelationID string    `json:"correlation_id"`
    CausationID   string    `json:"causation_id"`
}

func (e BaseEvent) EventType() string      { return e.Type }
func (e BaseEvent) AggregateID() string    { return e.AggregateId }
func (e BaseEvent) AggregateType() string  { return "Order" }
func (e BaseEvent) Version() int           { return e.AggregateVer }
func (e BaseEvent) Timestamp() time.Time   { return e.OccurredAt }
```

```go
// command-side/order-command-service/events/order_created.go
package events

type OrderCreated struct {
    BaseEvent
    UserID     string      `json:"user_id"`
    Items      []OrderItem `json:"items"`
    TotalCents int64       `json:"total_cents"`
    Currency   string      `json:"currency"`
}

type OrderConfirmed struct {
    BaseEvent
    ConfirmedBy string `json:"confirmed_by"`
}

type OrderCancelled struct {
    BaseEvent
    Reason     string `json:"reason"`
    CancelledBy string `json:"cancelled_by"`
}

type ItemAdded struct {
    BaseEvent
    ProductID  string `json:"product_id"`
    Quantity   int    `json:"quantity"`
    PriceCents int64  `json:"price_cents"`
}

type ItemRemoved struct {
    BaseEvent
    ProductID string `json:"product_id"`
}
```

### Phase 2: Aggregate Root (Day 2-3)

```go
// command-side/order-command-service/aggregate/order.go
package aggregate

import (
    "errors"
    "time"

    "github.com/google/uuid"
    "order-command/events"
)

type OrderStatus string

const (
    OrderPending   OrderStatus = "PENDING"
    OrderConfirmed OrderStatus = "CONFIRMED"
    OrderCancelled OrderStatus = "CANCELLED"
)

type OrderItem struct {
    ProductID  string
    Quantity   int
    PriceCents int64
}

// Order is the aggregate root
type Order struct {
    id         string
    userID     string
    items      []OrderItem
    totalCents int64
    status     OrderStatus
    version    int
    changes    []events.DomainEvent // Uncommitted events
}

// --- Command methods (validate + apply) ---

func NewOrder(userID string, items []OrderItem) (*Order, error) {
    if len(items) == 0 {
        return nil, errors.New("order must have at least one item")
    }

    var total int64
    for _, item := range items {
        total += item.PriceCents * int64(item.Quantity)
    }

    o := &Order{}
    o.apply(events.OrderCreated{
        BaseEvent: events.BaseEvent{
            ID:           uuid.New().String(),
            Type:         "OrderCreated",
            AggregateId:  uuid.New().String(),
            AggregateVer: 1,
            OccurredAt:   time.Now(),
        },
        UserID:     userID,
        Items:      toEventItems(items),
        TotalCents: total,
        Currency:   "USD",
    })

    return o, nil
}

func (o *Order) Confirm(confirmedBy string) error {
    if o.status != OrderPending {
        return errors.New("can only confirm pending orders")
    }

    o.apply(events.OrderConfirmed{
        BaseEvent: events.BaseEvent{
            ID:           uuid.New().String(),
            Type:         "OrderConfirmed",
            AggregateId:  o.id,
            AggregateVer: o.version + 1,
            OccurredAt:   time.Now(),
        },
        ConfirmedBy: confirmedBy,
    })
    return nil
}

func (o *Order) Cancel(reason, cancelledBy string) error {
    if o.status == OrderCancelled {
        return errors.New("order already cancelled")
    }

    o.apply(events.OrderCancelled{
        BaseEvent: events.BaseEvent{
            ID:           uuid.New().String(),
            Type:         "OrderCancelled",
            AggregateId:  o.id,
            AggregateVer: o.version + 1,
            OccurredAt:   time.Now(),
        },
        Reason:      reason,
        CancelledBy: cancelledBy,
    })
    return nil
}

// --- Event application (state mutation) ---

func (o *Order) apply(event events.DomainEvent) {
    o.when(event)
    o.changes = append(o.changes, event)
}

func (o *Order) when(event events.DomainEvent) {
    switch e := event.(type) {
    case events.OrderCreated:
        o.id = e.AggregateID()
        o.userID = e.UserID
        o.items = fromEventItems(e.Items)
        o.totalCents = e.TotalCents
        o.status = OrderPending
        o.version = e.Version()
    case events.OrderConfirmed:
        o.status = OrderConfirmed
        o.version = e.Version()
    case events.OrderCancelled:
        o.status = OrderCancelled
        o.version = e.Version()
    }
}

// Rebuild aggregate from event history
func LoadFromHistory(eventHistory []events.DomainEvent) *Order {
    o := &Order{}
    for _, event := range eventHistory {
        o.when(event)
    }
    return o
}

func (o *Order) UncommittedChanges() []events.DomainEvent {
    return o.changes
}

func (o *Order) MarkChangesCommitted() {
    o.changes = nil
}
```

### Phase 3: Event Store (Kafka-backed) (Day 4-5)

```go
// command-side/order-command-service/store/event_store.go
package store

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/segmentio/kafka-go"
    "order-command/events"
)

type EventStore struct {
    writer *kafka.Writer
    reader *kafka.Reader
}

func NewEventStore(brokers []string) *EventStore {
    return &EventStore{
        writer: &kafka.Writer{
            Addr:         kafka.TCP(brokers...),
            Topic:        "order.event-store",
            Balancer:     &kafka.Hash{},
            RequiredAcks: kafka.RequireAll,
        },
    }
}

// Append events for an aggregate
func (s *EventStore) Save(ctx context.Context, aggregateID string, evts []events.DomainEvent, expectedVersion int) error {
    messages := make([]kafka.Message, len(evts))
    for i, evt := range evts {
        data, err := json.Marshal(evt)
        if err != nil {
            return fmt.Errorf("marshal event: %w", err)
        }

        messages[i] = kafka.Message{
            Key:   []byte(aggregateID),
            Value: data,
            Headers: []kafka.Header{
                {Key: "event-type", Value: []byte(evt.EventType())},
                {Key: "aggregate-id", Value: []byte(aggregateID)},
                {Key: "version", Value: []byte(fmt.Sprintf("%d", evt.Version()))},
            },
        }
    }
    return s.writer.WriteMessages(ctx, messages...)
}

// Load all events for an aggregate
func (s *EventStore) Load(ctx context.Context, aggregateID string) ([]events.DomainEvent, error) {
    // Read from a dedicated reader for this aggregate
    // In production, you'd use a state store or compacted topic
    // For learning: scan the topic filtered by key
    var result []events.DomainEvent
    // ... implementation depends on your read strategy
    return result, nil
}
```

### Phase 4: Snapshot Store (Day 5)

```go
// command-side/order-command-service/store/snapshot_store.go
package store

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/redis/go-redis/v9"
)

type OrderSnapshot struct {
    ID         string `json:"id"`
    UserID     string `json:"user_id"`
    Status     string `json:"status"`
    TotalCents int64  `json:"total_cents"`
    Version    int    `json:"version"`
}

type SnapshotStore struct {
    client *redis.Client
}

const snapshotInterval = 10 // Snapshot every 10 events

func (s *SnapshotStore) Save(ctx context.Context, snapshot OrderSnapshot) error {
    data, _ := json.Marshal(snapshot)
    key := fmt.Sprintf("snapshot:order:%s", snapshot.ID)
    return s.client.Set(ctx, key, data, 0).Err()
}

func (s *SnapshotStore) Load(ctx context.Context, aggregateID string) (*OrderSnapshot, error) {
    key := fmt.Sprintf("snapshot:order:%s", aggregateID)
    data, err := s.client.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, nil // No snapshot, replay from beginning
    }
    if err != nil {
        return nil, err
    }
    var snap OrderSnapshot
    json.Unmarshal(data, &snap)
    return &snap, nil
}

// Load aggregate: snapshot + events after snapshot version
// This avoids replaying ALL events for long-lived aggregates
```

### Phase 5: Projections (Query Side) (Day 6-8)

```go
// query-side/order-query-service/projections/order_list.go
package projections

import (
    "context"
    "encoding/json"
    "log"

    "github.com/elastic/go-elasticsearch/v8"
    "github.com/segmentio/kafka-go"
)

type OrderView struct {
    ID         string `json:"id"`
    UserID     string `json:"user_id"`
    Status     string `json:"status"`
    TotalCents int64  `json:"total_cents"`
    ItemCount  int    `json:"item_count"`
    CreatedAt  string `json:"created_at"`
    UpdatedAt  string `json:"updated_at"`
}

type OrderListProjection struct {
    es     *elasticsearch.Client
    reader *kafka.Reader
}

func (p *OrderListProjection) Start(ctx context.Context) {
    for {
        msg, err := p.reader.FetchMessage(ctx)
        if err != nil {
            if ctx.Err() != nil {
                return
            }
            continue
        }

        eventType := getHeader(msg.Headers, "event-type")
        aggregateID := getHeader(msg.Headers, "aggregate-id")

        switch eventType {
        case "OrderCreated":
            p.handleOrderCreated(ctx, aggregateID, msg.Value)
        case "OrderConfirmed":
            p.handleOrderConfirmed(ctx, aggregateID)
        case "OrderCancelled":
            p.handleOrderCancelled(ctx, aggregateID, msg.Value)
        }

        p.reader.CommitMessages(ctx, msg)
    }
}

func (p *OrderListProjection) handleOrderCreated(ctx context.Context, id string, data []byte) {
    var event struct {
        UserID     string `json:"user_id"`
        Items      []any  `json:"items"`
        TotalCents int64  `json:"total_cents"`
        OccurredAt string `json:"occurred_at"`
    }
    json.Unmarshal(data, &event)

    view := OrderView{
        ID:         id,
        UserID:     event.UserID,
        Status:     "PENDING",
        TotalCents: event.TotalCents,
        ItemCount:  len(event.Items),
        CreatedAt:  event.OccurredAt,
        UpdatedAt:  event.OccurredAt,
    }

    body, _ := json.Marshal(view)
    p.es.Index("orders", bytes.NewReader(body),
        p.es.Index.WithDocumentID(id),
        p.es.Index.WithContext(ctx),
    )
}

func (p *OrderListProjection) handleOrderConfirmed(ctx context.Context, id string) {
    // Partial update in Elasticsearch
    update := map[string]any{
        "doc": map[string]any{
            "status":    "CONFIRMED",
            "updated_at": time.Now().Format(time.RFC3339),
        },
    }
    body, _ := json.Marshal(update)
    p.es.Update("orders", id, bytes.NewReader(body), p.es.Update.WithContext(ctx))
}
```

```go
// query-side/order-query-service/projections/revenue_stats.go
package projections

// Real-time revenue dashboard projection -> Redis
type RevenueProjection struct {
    redis *redis.Client
}

func (p *RevenueProjection) handleOrderConfirmed(ctx context.Context, orderID string, totalCents int64) {
    today := time.Now().Format("2006-01-02")

    pipe := p.redis.Pipeline()
    pipe.IncrBy(ctx, fmt.Sprintf("revenue:daily:%s", today), totalCents)
    pipe.Incr(ctx, fmt.Sprintf("orders:daily:%s", today))
    pipe.IncrBy(ctx, "revenue:total", totalCents)
    pipe.Incr(ctx, "orders:total")
    pipe.Exec(ctx)
}
```

### Phase 6: Event Replay Utility (Day 9)

```go
// tools/replay-events.go
// Rebuild all projections from scratch by replaying the event store
package main

func main() {
    // 1. Reset Elasticsearch index (delete + recreate with mapping)
    // 2. Reset Redis keys
    // 3. Create a new Kafka consumer starting from offset 0
    // 4. Replay all events through the projection handlers
    // 5. Log progress: "Replayed 10000/50000 events..."
    //
    // This is the killer feature of event sourcing:
    // you can rebuild any read model at any time.
}
```

---

## Validation Checklist

- [ ] Command API accepts CreateOrder, ConfirmOrder, CancelOrder
- [ ] Commands validate business rules before appending events
- [ ] Events stored in Kafka topic with aggregate_id as key
- [ ] Aggregate rebuilt from event history (LoadFromHistory works)
- [ ] Snapshots taken every N events, reducing replay time
- [ ] Elasticsearch projection: search orders by user, status, date range
- [ ] Redis projection: real-time revenue/order count dashboard
- [ ] Query API serves data from read models (not event store)
- [ ] Event replay tool rebuilds all projections from scratch
- [ ] Command + Query services scale independently
- [ ] Traces show command -> event store -> projection pipeline
- [ ] Eventual consistency: write on command side visible on query side within seconds

---

## Key Concepts to Internalize

1. **Event Sourcing**: state = replay of events. No UPDATE/DELETE, only INSERT.
2. **Aggregate Root**: consistency boundary. All mutations go through it.
3. **CQRS split**: command side optimized for writes, query side for reads. Different schemas.
4. **Projections**: event consumers that build read-optimized views (materialized views)
5. **Snapshots**: periodically save aggregate state to avoid replaying thousands of events
6. **Temporal queries**: "what was the order state at 3pm yesterday?" -- replay events up to that timestamp
7. **Projection rebuild**: if your read model is wrong, drop it and replay. That's the power.
