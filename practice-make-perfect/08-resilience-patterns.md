# Project 08: Resilience Patterns for AI Fallbacks
# TIER 2 -- Production Patterns

## Goal
Implement circuit breaker, retry with backoff, bulkhead, timeout, and
intelligent fallback patterns for AI services at both application level (code)
and infrastructure level (service mesh). Ensure chat system remains functional
when AI services fail by implementing graceful degradation and backup responses.

---

## Architecture

```
  [order-service]
       |
  +----+----+----+
  |         |         |
  Circuit   Bulkhead  Timeout
  Breaker   (isolated (5s max)
  (5 fails  pools)
   = open)
  |         |         |
[payment] [inventory] [shipping]
```

---

## Directory Structure

```
08-resilience-patterns/
├── app-level/
│   ├── go/
│   │   ├── circuit_breaker.go
│   │   ├── retry.go
│   │   ├── bulkhead.go
│   │   ├── timeout.go
│   │   └── fallback.go
│   ├── typescript/
│   │   ├── circuit-breaker.ts
│   │   ├── retry.ts
│   │   └── bulkhead.ts
│   └── python/
│       ├── circuit_breaker.py
│       └── retry.py
├── mesh-level/
│   ├── istio/
│   │   ├── circuit-breaker.yaml
│   │   ├── retry-policy.yaml
│   │   └── connection-pool.yaml
│   └── linkerd/
│       └── service-profile-retries.yaml
├── testing/
│   ├── cascading-failure-test.sh
│   ├── circuit-breaker-test.sh
│   ├── load-test/
│   │   └── k6-script.js
│   └── chaos/
│       └── kill-payment.yaml
└── dashboards/
    └── resilience-dashboard.json
```

---

## Step-by-Step Implementation

### Phase 1: Circuit Breaker (Day 1-3)

```go
// app-level/go/circuit_breaker.go
package resilience

import (
    "errors"
    "sync"
    "time"
)

type State int

const (
    StateClosed   State = iota // Normal operation
    StateOpen                   // Failing, reject requests
    StateHalfOpen              // Testing if service recovered
)

type CircuitBreaker struct {
    mu               sync.Mutex
    state            State
    failureCount     int
    successCount     int
    failureThreshold int
    successThreshold int      // Required successes in half-open to close
    timeout          time.Duration  // How long to stay open before half-open
    lastFailureTime  time.Time
    onStateChange    func(from, to State)
}

var ErrCircuitOpen = errors.New("circuit breaker is open")

func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
    return &CircuitBreaker{
        state:            StateClosed,
        failureThreshold: failureThreshold,
        successThreshold: successThreshold,
        timeout:          timeout,
    }
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
    cb.mu.Lock()

    switch cb.state {
    case StateOpen:
        if time.Since(cb.lastFailureTime) > cb.timeout {
            cb.setState(StateHalfOpen)
        } else {
            cb.mu.Unlock()
            return ErrCircuitOpen
        }
    }

    cb.mu.Unlock()

    // Execute the function
    err := fn()

    cb.mu.Lock()
    defer cb.mu.Unlock()

    if err != nil {
        cb.failureCount++
        cb.lastFailureTime = time.Now()

        if cb.state == StateHalfOpen {
            cb.setState(StateOpen)
        } else if cb.failureCount >= cb.failureThreshold {
            cb.setState(StateOpen)
        }
        return err
    }

    // Success
    if cb.state == StateHalfOpen {
        cb.successCount++
        if cb.successCount >= cb.successThreshold {
            cb.setState(StateClosed)
        }
    }
    cb.failureCount = 0
    return nil
}

func (cb *CircuitBreaker) setState(new State) {
    old := cb.state
    cb.state = new
    cb.failureCount = 0
    cb.successCount = 0
    if cb.onStateChange != nil {
        cb.onStateChange(old, new)
    }
}

// Usage in order-service:
//
//   paymentCB := NewCircuitBreaker(5, 3, 30*time.Second)
//   err := paymentCB.Execute(func() error {
//       return paymentClient.Charge(ctx, amount)
//   })
//   if errors.Is(err, ErrCircuitOpen) {
//       return fallbackResponse() // Return cached or default response
//   }
```

### Phase 2: Retry with Exponential Backoff + Jitter (Day 3-4)

```go
// app-level/go/retry.go
package resilience

import (
    "context"
    "math"
    "math/rand"
    "time"
)

type RetryConfig struct {
    MaxAttempts    int
    InitialDelay   time.Duration
    MaxDelay       time.Duration
    BackoffFactor  float64
    RetryableCheck func(error) bool // Which errors to retry
}

func DefaultRetryConfig() RetryConfig {
    return RetryConfig{
        MaxAttempts:   3,
        InitialDelay:  100 * time.Millisecond,
        MaxDelay:      5 * time.Second,
        BackoffFactor: 2.0,
        RetryableCheck: func(err error) bool {
            // Only retry transient errors (network, 503, timeout)
            return IsTransient(err)
        },
    }
}

func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
    var lastErr error

    for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
        if err := ctx.Err(); err != nil {
            return err // Context cancelled
        }

        lastErr = fn()
        if lastErr == nil {
            return nil
        }

        if !cfg.RetryableCheck(lastErr) {
            return lastErr // Non-retryable error
        }

        if attempt < cfg.MaxAttempts-1 {
            delay := calculateBackoff(attempt, cfg)
            select {
            case <-time.After(delay):
            case <-ctx.Done():
                return ctx.Err()
            }
        }
    }
    return lastErr
}

func calculateBackoff(attempt int, cfg RetryConfig) time.Duration {
    // Exponential backoff
    delay := float64(cfg.InitialDelay) * math.Pow(cfg.BackoffFactor, float64(attempt))

    // Cap at max delay
    if delay > float64(cfg.MaxDelay) {
        delay = float64(cfg.MaxDelay)
    }

    // Add jitter (0.5x to 1.5x) to prevent thundering herd
    jitter := 0.5 + rand.Float64()
    delay *= jitter

    return time.Duration(delay)
}
```

### Phase 3: Bulkhead (Day 5-6)

```go
// app-level/go/bulkhead.go
package resilience

import (
    "context"
    "errors"
    "time"
)

var ErrBulkheadFull = errors.New("bulkhead: max concurrent requests reached")

// Semaphore-based bulkhead: limits concurrent calls to a dependency
type Bulkhead struct {
    name    string
    sem     chan struct{}
    timeout time.Duration
}

func NewBulkhead(name string, maxConcurrent int, timeout time.Duration) *Bulkhead {
    return &Bulkhead{
        name:    name,
        sem:     make(chan struct{}, maxConcurrent),
        timeout: timeout,
    }
}

func (b *Bulkhead) Execute(ctx context.Context, fn func() error) error {
    // Try to acquire a slot
    select {
    case b.sem <- struct{}{}:
        defer func() { <-b.sem }()
        return fn()
    case <-time.After(b.timeout):
        return ErrBulkheadFull
    case <-ctx.Done():
        return ctx.Err()
    }
}

// Usage: isolate each downstream dependency
//
//   paymentBulkhead   := NewBulkhead("payment", 20, 5*time.Second)
//   inventoryBulkhead := NewBulkhead("inventory", 30, 5*time.Second)
//   shippingBulkhead  := NewBulkhead("shipping", 10, 5*time.Second)
//
//   // Payment calls can't exhaust the thread pool and starve inventory calls
//   err := paymentBulkhead.Execute(ctx, func() error {
//       return paymentClient.Charge(ctx, amount)
//   })
```

### Phase 4: Timeout + Fallback (Day 7)

```go
// app-level/go/timeout.go
package resilience

import (
    "context"
    "time"
)

func WithTimeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
    ctx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()
    return fn(ctx)
}

// app-level/go/fallback.go
package resilience

func WithFallback[T any](primary func() (T, error), fallback func() (T, error)) (T, error) {
    result, err := primary()
    if err != nil {
        return fallback()
    }
    return result, nil
}

// Usage: cached product data as fallback when product-service is down
//
//   products, err := WithFallback(
//       func() ([]Product, error) { return productClient.List(ctx) },
//       func() ([]Product, error) { return cache.GetProducts(ctx) },
//   )
```

### Phase 5: Compose All Patterns (Day 8)

```go
// Compose: timeout -> bulkhead -> circuit breaker -> retry -> call
func (s *OrderService) callPaymentService(ctx context.Context, req PaymentRequest) (*PaymentResponse, error) {
    var resp *PaymentResponse

    // Layer 1: Timeout (hard limit)
    err := WithTimeout(ctx, 10*time.Second, func(ctx context.Context) error {

        // Layer 2: Bulkhead (limit concurrency)
        return s.paymentBulkhead.Execute(ctx, func() error {

            // Layer 3: Circuit breaker (fail fast if service is down)
            return s.paymentCB.Execute(func() error {

                // Layer 4: Retry (handle transient failures)
                return Retry(ctx, DefaultRetryConfig(), func() error {
                    var err error
                    resp, err = s.paymentClient.Charge(ctx, req)
                    return err
                })
            })
        })
    })

    if err != nil {
        // Fallback: queue for later processing
        s.paymentQueue.Enqueue(req)
        return &PaymentResponse{Status: "PENDING"}, nil
    }

    return resp, nil
}
```

### Phase 6: Mesh-Level Resilience (Day 9-10)

```yaml
# mesh-level/istio/circuit-breaker.yaml
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: payment-service-resilience
  namespace: ecommerce
spec:
  host: payment-service
  trafficPolicy:
    connectionPool:
      tcp:
        maxConnections: 50        # Bulkhead: max TCP connections
      http:
        http1MaxPendingRequests: 50  # Bulkhead: max queued requests
        http2MaxRequests: 100     # Bulkhead: max active requests
        maxRequestsPerConnection: 10
        maxRetries: 3             # Retry limit
    outlierDetection:             # Circuit breaker
      consecutive5xxErrors: 5     # 5 errors = eject
      interval: 10s
      baseEjectionTime: 30s
      maxEjectionPercent: 50
```

### Phase 7: Load Test + Validate (Day 11-12)

```javascript
// testing/load-test/k6-script.js
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '1m', target: 50 },   // Ramp up
    { duration: '3m', target: 50 },   // Sustain
    { duration: '1m', target: 200 },  // Spike
    { duration: '2m', target: 200 },  // Sustain spike
    { duration: '1m', target: 0 },    // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],  // 95% under 2s
    http_req_failed: ['rate<0.05'],     // <5% error rate
  },
};

export default function () {
  const res = http.post('http://localhost:8080/api/v1/orders', JSON.stringify({
    user_id: 'user-1',
    items: [{ product_id: 'prod-1', quantity: 1 }],
  }), { headers: { 'Content-Type': 'application/json' } });

  check(res, {
    'status is 200 or 202': (r) => r.status === 200 || r.status === 202,
    'response time < 2000ms': (r) => r.timings.duration < 2000,
  });

  sleep(0.1);
}
```

---

## Validation Checklist

- [ ] Circuit breaker opens after 5 consecutive failures
- [ ] Circuit breaker transitions: closed -> open -> half-open -> closed
- [ ] Retry uses exponential backoff with jitter
- [ ] Retry only retries transient errors (not 400, 404)
- [ ] Bulkhead limits concurrent requests per dependency
- [ ] Bulkhead rejects with ErrBulkheadFull when full (doesn't hang)
- [ ] Timeout kills slow requests after configured duration
- [ ] Fallback returns cached/default data when primary fails
- [ ] Composed: timeout -> bulkhead -> circuit breaker -> retry
- [ ] Mesh-level circuit breaker ejects unhealthy pods
- [ ] Load test: system stays responsive during spike
- [ ] Kill payment-service: order-service degrades gracefully, not cascading failure
- [ ] Grafana dashboard shows circuit breaker state changes

---

## Key Concepts to Internalize

1. **Circuit breaker**: prevent wasting resources on calls that will fail. "Fail fast."
2. **Retry + jitter**: retries without jitter cause thundering herd. Always add randomness.
3. **Bulkhead**: isolate failures. One slow dependency can't consume all resources.
4. **Composition order matters**: timeout wraps bulkhead wraps circuit breaker wraps retry
5. **App-level vs mesh-level**: use both. Mesh handles L7 retries/circuit breaking. App handles business logic fallbacks.
6. **Cascading failure**: Service A slow -> Service B waits -> Service B's pool exhausted -> Service C can't reach B -> entire system down. Resilience patterns break this chain.
