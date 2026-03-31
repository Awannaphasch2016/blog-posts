# Project 06: Saga Pattern for AI Workflow Orchestration
# TIER 2 -- Production Patterns

## Goal
Implement complex AI conversation workflows as distributed sagas using both
choreography (event-driven) and orchestration (Temporal) approaches.
Handle AI service failures, context management, and conversation flow
compensation with proper rollback mechanisms.

---

## Architecture

### Choreography Saga
```
OrderCreated ──> [Inventory] ──> StockReserved ──> [Payment] ──> PaymentProcessed ──> [Shipping]
                      |                                 |
                StockInsufficient               PaymentFailed
                      |                                 |
                      v                                 v
                [Order: Cancel]                [Inventory: Release Stock]
```

### Orchestration Saga (Temporal)
```
[Temporal Workflow Engine]
    |
    ├── Step 1: ReserveStock(order)        ── compensate: ReleaseStock(order)
    ├── Step 2: ProcessPayment(order)      ── compensate: RefundPayment(order)
    ├── Step 3: CreateShipment(order)      ── compensate: CancelShipment(order)
    └── Step 4: ConfirmOrder(order)

    On failure at any step -> run compensations in reverse order
```

---

## Directory Structure

```
06-saga-pattern/
├── choreography/
│   ├── order-service/
│   │   ├── saga_state.go           # Track saga progress
│   │   └── compensations.go       # React to failure events
│   ├── inventory-service/
│   │   ├── reserve_handler.go
│   │   └── release_handler.go     # Compensation
│   ├── payment-service/
│   │   ├── charge_handler.go
│   │   └── refund_handler.go      # Compensation
│   └── shipping-service/
│       ├── dispatch_handler.go
│       └── cancel_handler.go      # Compensation
├── orchestration/
│   ├── temporal/
│   │   ├── temporal-values.yaml   # Helm values
│   │   └── namespace.yaml
│   ├── workflows/
│   │   ├── order_workflow.go      # Saga orchestrator
│   │   └── order_workflow_test.go
│   ├── activities/
│   │   ├── inventory.go           # ReserveStock / ReleaseStock
│   │   ├── payment.go             # ProcessPayment / RefundPayment
│   │   ├── shipping.go            # CreateShipment / CancelShipment
│   │   └── notification.go        # NotifyUser
│   ├── worker/
│   │   └── main.go                # Temporal worker registration
│   └── starter/
│       └── main.go                # Starts workflow on order creation
├── k8s/
│   ├── temporal/
│   └── services/
└── testing/
    ├── happy_path_test.go
    ├── payment_failure_test.go
    └── inventory_failure_test.go
```

---

## Step-by-Step Implementation

### Phase 1: Choreography Saga (Day 1-5)

**Saga state tracking in order-service:**
```go
// choreography/order-service/saga_state.go
package saga

import (
    "context"
    "fmt"
    "time"
)

type SagaStep string

const (
    StepStockReservation SagaStep = "STOCK_RESERVATION"
    StepPayment          SagaStep = "PAYMENT"
    StepShipping         SagaStep = "SHIPPING"
)

type SagaStatus string

const (
    SagaPending      SagaStatus = "PENDING"
    SagaCompleted    SagaStatus = "COMPLETED"
    SagaCompensating SagaStatus = "COMPENSATING"
    SagaFailed       SagaStatus = "FAILED"
)

type SagaState struct {
    OrderID     string                `json:"order_id"`
    Status      SagaStatus            `json:"status"`
    Steps       map[SagaStep]StepState `json:"steps"`
    StartedAt   time.Time             `json:"started_at"`
    CompletedAt *time.Time            `json:"completed_at,omitempty"`
}

type StepState struct {
    Status    string     `json:"status"` // pending, completed, failed, compensated
    Timestamp time.Time  `json:"timestamp"`
    Error     string     `json:"error,omitempty"`
}

// Saga state stored in Redis for fast lookup
type SagaStateStore struct {
    redis *redis.Client
}

func (s *SagaStateStore) UpdateStep(ctx context.Context, orderID string, step SagaStep, status string, err error) {
    // Update the step status
    // If any step fails, trigger compensation
}
```

**Compensation handlers:**
```go
// choreography/order-service/compensations.go
package saga

// Listens for failure events and triggers compensating transactions

func (h *CompensationHandler) HandlePaymentFailed(ctx context.Context, event PaymentFailedEvent) error {
    // 1. Publish StockReleaseRequested -> inventory-service will release stock
    // 2. Update order status to CANCELLED
    // 3. Publish OrderCancelled -> notification-service notifies user
    return nil
}

func (h *CompensationHandler) HandleStockInsufficient(ctx context.Context, event StockInsufficientEvent) error {
    // 1. Update order status to CANCELLED (no stock was reserved, so no compensation needed)
    // 2. Publish OrderCancelled -> notification-service notifies user
    return nil
}

func (h *CompensationHandler) HandleShipmentFailed(ctx context.Context, event ShipmentFailedEvent) error {
    // 1. Publish RefundRequested -> payment-service refunds
    // 2. Publish StockReleaseRequested -> inventory-service releases
    // 3. Update order status to CANCELLED
    return nil
}
```

**Inventory compensation:**
```go
// choreography/inventory-service/release_handler.go
package handlers

func (h *InventoryHandler) HandleStockReleaseRequested(ctx context.Context, event StockReleaseRequestedEvent) error {
    // Idempotency: check if stock already released for this order
    released, _ := h.repo.IsStockReleased(ctx, event.OrderID)
    if released {
        return nil // Already compensated
    }

    // Release reserved stock
    for _, item := range event.Items {
        h.repo.ReleaseStock(ctx, item.ProductID, item.Quantity)
    }

    // Mark as released (idempotency key)
    h.repo.MarkStockReleased(ctx, event.OrderID)

    // Publish StockReleased event
    h.producer.Publish(ctx, StockReleasedEvent{OrderID: event.OrderID})
    return nil
}
```

### Phase 2: Orchestration Saga with Temporal (Day 6-10)

```bash
# Install Temporal on K8s
helm repo add temporal https://charts.temporal.io
helm install temporal temporal/temporal \
  -n temporal --create-namespace \
  --set server.replicaCount=1 \
  --set cassandra.config.cluster_size=1 \
  --set prometheus.enabled=false \
  --set grafana.enabled=false \
  --set elasticsearch.enabled=false
```

**Saga workflow definition:**
```go
// orchestration/workflows/order_workflow.go
package workflows

import (
    "fmt"
    "time"

    "go.temporal.io/sdk/temporal"
    "go.temporal.io/sdk/workflow"
    "order-saga/activities"
)

type OrderSagaInput struct {
    OrderID string
    UserID  string
    Items   []OrderItem
    Total   int64
}

type OrderSagaResult struct {
    OrderID    string
    Status     string
    ShipmentID string
}

func OrderSagaWorkflow(ctx workflow.Context, input OrderSagaInput) (*OrderSagaResult, error) {
    logger := workflow.GetLogger(ctx)

    // Activity options with timeouts and retries
    activityOpts := workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
        RetryPolicy: &temporal.RetryPolicy{
            InitialInterval:    time.Second,
            BackoffCoefficient: 2.0,
            MaximumInterval:    30 * time.Second,
            MaximumAttempts:    3,
        },
    }
    ctx = workflow.WithActivityOptions(ctx, activityOpts)

    // Track compensations to run on failure
    var compensations []func(ctx workflow.Context) error

    // --- Step 1: Reserve Stock ---
    logger.Info("Reserving stock", "orderID", input.OrderID)
    var reserveResult activities.ReserveStockResult
    err := workflow.ExecuteActivity(ctx, activities.ReserveStock, activities.ReserveStockInput{
        OrderID: input.OrderID,
        Items:   input.Items,
    }).Get(ctx, &reserveResult)

    if err != nil {
        logger.Error("Stock reservation failed", "error", err)
        return nil, fmt.Errorf("reserve stock: %w", err)
    }

    // Register compensation
    compensations = append(compensations, func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, activities.ReleaseStock, activities.ReleaseStockInput{
            OrderID:       input.OrderID,
            ReservationID: reserveResult.ReservationID,
        }).Get(ctx, nil)
    })

    // --- Step 2: Process Payment ---
    logger.Info("Processing payment", "orderID", input.OrderID)
    var paymentResult activities.PaymentResult
    err = workflow.ExecuteActivity(ctx, activities.ProcessPayment, activities.PaymentInput{
        OrderID: input.OrderID,
        UserID:  input.UserID,
        Amount:  input.Total,
    }).Get(ctx, &paymentResult)

    if err != nil {
        logger.Error("Payment failed, compensating", "error", err)
        runCompensations(ctx, compensations)
        return nil, fmt.Errorf("process payment: %w", err)
    }

    compensations = append(compensations, func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, activities.RefundPayment, activities.RefundInput{
            PaymentID: paymentResult.PaymentID,
            OrderID:   input.OrderID,
        }).Get(ctx, nil)
    })

    // --- Step 3: Create Shipment ---
    logger.Info("Creating shipment", "orderID", input.OrderID)
    var shipmentResult activities.ShipmentResult
    err = workflow.ExecuteActivity(ctx, activities.CreateShipment, activities.ShipmentInput{
        OrderID: input.OrderID,
        UserID:  input.UserID,
        Items:   input.Items,
    }).Get(ctx, &shipmentResult)

    if err != nil {
        logger.Error("Shipment failed, compensating", "error", err)
        runCompensations(ctx, compensations)
        return nil, fmt.Errorf("create shipment: %w", err)
    }

    // --- Step 4: Confirm Order ---
    logger.Info("Confirming order", "orderID", input.OrderID)
    err = workflow.ExecuteActivity(ctx, activities.ConfirmOrder, activities.ConfirmOrderInput{
        OrderID: input.OrderID,
    }).Get(ctx, nil)

    if err != nil {
        logger.Error("Order confirmation failed, compensating", "error", err)
        runCompensations(ctx, compensations)
        return nil, fmt.Errorf("confirm order: %w", err)
    }

    // --- Step 5: Notify User ---
    // Fire-and-forget, don't compensate on notification failure
    _ = workflow.ExecuteActivity(ctx, activities.NotifyUser, activities.NotifyInput{
        UserID:  input.UserID,
        OrderID: input.OrderID,
        Status:  "CONFIRMED",
    }).Get(ctx, nil)

    return &OrderSagaResult{
        OrderID:    input.OrderID,
        Status:     "CONFIRMED",
        ShipmentID: shipmentResult.ShipmentID,
    }, nil
}

func runCompensations(ctx workflow.Context, compensations []func(workflow.Context) error) {
    // Run compensations in reverse order
    for i := len(compensations) - 1; i >= 0; i-- {
        if err := compensations[i](ctx); err != nil {
            workflow.GetLogger(ctx).Error("Compensation failed", "step", i, "error", err)
            // Temporal will retry the compensation
        }
    }
}
```

**Activity implementations:**
```go
// orchestration/activities/inventory.go
package activities

import (
    "context"
    "fmt"
)

type ReserveStockInput struct {
    OrderID string
    Items   []OrderItem
}

type ReserveStockResult struct {
    ReservationID string
}

// Activities are the actual service calls
func ReserveStock(ctx context.Context, input ReserveStockInput) (*ReserveStockResult, error) {
    // Call inventory-service gRPC endpoint
    // The service checks stock and creates a reservation
    resp, err := inventoryClient.ReserveStock(ctx, &inventorypb.ReserveRequest{
        OrderId: input.OrderID,
        Items:   toProtoItems(input.Items),
    })
    if err != nil {
        return nil, fmt.Errorf("inventory gRPC: %w", err)
    }
    return &ReserveStockResult{ReservationID: resp.ReservationId}, nil
}

type ReleaseStockInput struct {
    OrderID       string
    ReservationID string
}

func ReleaseStock(ctx context.Context, input ReleaseStockInput) error {
    // Compensating action: release the reserved stock
    _, err := inventoryClient.ReleaseStock(ctx, &inventorypb.ReleaseRequest{
        OrderId:       input.OrderID,
        ReservationId: input.ReservationID,
    })
    return err
}
```

**Temporal worker:**
```go
// orchestration/worker/main.go
package main

import (
    "log"

    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/worker"
    "order-saga/activities"
    "order-saga/workflows"
)

func main() {
    c, err := client.Dial(client.Options{
        HostPort: "temporal-frontend.temporal:7233",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    w := worker.New(c, "order-saga-queue", worker.Options{})

    // Register workflow
    w.RegisterWorkflow(workflows.OrderSagaWorkflow)

    // Register activities
    w.RegisterActivity(activities.ReserveStock)
    w.RegisterActivity(activities.ReleaseStock)
    w.RegisterActivity(activities.ProcessPayment)
    w.RegisterActivity(activities.RefundPayment)
    w.RegisterActivity(activities.CreateShipment)
    w.RegisterActivity(activities.CancelShipment)
    w.RegisterActivity(activities.ConfirmOrder)
    w.RegisterActivity(activities.NotifyUser)

    if err := w.Run(worker.InterruptCh()); err != nil {
        log.Fatal(err)
    }
}
```

### Phase 3: Workflow Starter (Day 11)

```go
// orchestration/starter/main.go
// Called by order-service when a new order is placed

func StartOrderSaga(ctx context.Context, temporalClient client.Client, order *Order) (string, error) {
    options := client.StartWorkflowOptions{
        ID:        fmt.Sprintf("order-saga-%s", order.ID),
        TaskQueue: "order-saga-queue",
        // Workflow timeout: if not done in 1 hour, something is very wrong
        WorkflowExecutionTimeout: time.Hour,
    }

    run, err := temporalClient.ExecuteWorkflow(ctx, options, workflows.OrderSagaWorkflow, workflows.OrderSagaInput{
        OrderID: order.ID,
        UserID:  order.UserID,
        Items:   order.Items,
        Total:   order.TotalCents,
    })
    if err != nil {
        return "", err
    }

    return run.GetRunID(), nil
}
```

---

## Testing Failure Scenarios

```go
// testing/payment_failure_test.go
func TestSaga_PaymentFails_StockReleased(t *testing.T) {
    // 1. Create an order
    // 2. Inventory reserves stock successfully
    // 3. Payment FAILS (simulate with mock)
    // 4. Verify: stock is released (compensation ran)
    // 5. Verify: order status is CANCELLED
    // 6. Verify: user notified of cancellation
}

func TestSaga_ShipmentFails_PaymentRefunded(t *testing.T) {
    // 1. Create an order
    // 2. Inventory reserves stock successfully
    // 3. Payment succeeds
    // 4. Shipment FAILS
    // 5. Verify: payment is refunded (compensation ran)
    // 6. Verify: stock is released (compensation ran)
    // 7. Verify: order status is CANCELLED
}

func TestSaga_HappyPath(t *testing.T) {
    // 1. Create an order
    // 2. All steps succeed
    // 3. Verify: order status is CONFIRMED
    // 4. Verify: shipment created
    // 5. Verify: user notified of confirmation
}
```

---

## Validation Checklist

### Choreography
- [ ] OrderCreated triggers inventory reservation
- [ ] StockReserved triggers payment processing
- [ ] PaymentProcessed triggers shipment creation
- [ ] PaymentFailed triggers stock release (compensation)
- [ ] StockInsufficient triggers order cancellation
- [ ] All compensations are idempotent
- [ ] Saga state tracked and queryable

### Orchestration (Temporal)
- [ ] Temporal cluster running on K8s
- [ ] Worker registered with all activities
- [ ] Happy path: order -> confirmed in Temporal UI
- [ ] Payment failure: compensations run in reverse order
- [ ] Shipment failure: payment refunded + stock released
- [ ] Temporal UI shows workflow execution history
- [ ] Activity retries work (kill a service, watch retry)
- [ ] Workflow survives worker restart (Temporal durability)

---

## Key Concepts to Internalize

1. **Choreography vs Orchestration**: choreography = decoupled but hard to track; orchestration = centralized but easier to reason about
2. **Compensating transactions**: the "undo" for each step. Must be idempotent.
3. **Idempotency everywhere**: every handler must handle being called twice with the same input
4. **Temporal durable execution**: workflow state survives process crashes, deployments, even cluster restarts
5. **Saga != ACID**: sagas provide eventual consistency, not isolation. Design for this.
6. **Timeout handling**: what if a step never responds? Both approaches need timeout + compensation.
