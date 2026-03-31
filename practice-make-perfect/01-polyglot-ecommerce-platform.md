# Project 01: Polyglot Chat Platform
# TIER 1 -- Foundation

## Goal
Build a fully functional ChatGPT-like platform as 6-8 microservices in at least
3 different languages, each with its own database, supporting real-time chat,
AI integration, and conversation management on local Kubernetes.

---

## Services to Build

| Service | Language | Database | Port | Responsibility |
|---------|----------|----------|------|---------------|
| **user-service** | Go | PostgreSQL | 8001 | Registration, login, user profiles |
| **chat-service** | TypeScript/Node | MongoDB | 8002 | Real-time messaging, WebSocket handling, conversation management |
| **ai-service** | Python (FastAPI) | Redis | 8003 | LLM integration, prompt processing, AI response generation |
| **history-service** | Go | PostgreSQL | 8004 | Conversation persistence, search, analytics |
| **moderation-service** | TypeScript/Node | PostgreSQL | 8005 | Content filtering, safety checks, policy enforcement |
| **analytics-service** | Python | Redis + ClickHouse | 8006 | Usage tracking, conversation insights, metrics |
| **websocket-gateway** | Go | Redis (session store) | 8007 | WebSocket connection management, real-time events |
| **api-bff** | TypeScript/Node | None | 8080 | Backend-for-frontend aggregator, REST + WebSocket proxy |

---

## Directory Structure

```
01-polyglot-chat-platform/
├── k8s/
│   ├── namespaces.yaml
│   ├── user-service/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   ├── configmap.yaml
│   │   └── secret.yaml
│   ├── chat-service/
│   ├── ai-service/
│   ├── history-service/
│   ├── moderation-service/
│   ├── analytics-service/
│   ├── websocket-gateway/
│   ├── api-bff/
│   └── databases/
│       ├── postgres/
│       │   ├── statefulset.yaml
│       │   ├── service.yaml
│       │   └── pvc.yaml
│       ├── mongodb/
│       │   ├── statefulset.yaml
│       │   ├── service.yaml
│       │   └── pvc.yaml
│       ├── redis/
│       │   ├── deployment.yaml
│       │   └── service.yaml
│       └── clickhouse/
│           ├── statefulset.yaml
│           ├── service.yaml
│           └── pvc.yaml
├── services/
│   ├── user-service/          # Go
│   │   ├── Dockerfile
│   │   ├── go.mod
│   │   ├── main.go
│   │   ├── handlers/
│   │   ├── models/
│   │   └── repository/
│   ├── chat-service/          # TypeScript
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   └── src/
│   ├── ai-service/            # Python
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   ├── history-service/       # Go
│   ├── moderation-service/    # TypeScript
│   ├── analytics-service/     # Python
│   ├── websocket-gateway/     # Go
│   └── api-bff/              # TypeScript
├── helm/
│   └── chat-platform/
│       ├── Chart.yaml
│       ├── values.yaml
│       ├── values-dev.yaml
│       └── templates/
├── Tiltfile
├── skaffold.yaml
├── kind-config.yaml
└── ai-models/
    ├── openai-config.yaml
    ├── local-llm-setup.md
    └── vector-db-init/
```

---

## Step-by-Step Implementation

### Phase 1: Cluster + Databases (Day 1-2)

```bash
# Create Kind cluster with port mappings for WebSocket and HTTP traffic
cat <<EOF > kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30080
        hostPort: 8080    # HTTP API
      - containerPort: 30081
        hostPort: 8081    # WebSocket Gateway
  - role: worker
  - role: worker
EOF
kind create cluster --name chat-platform --config kind-config.yaml

# Create namespace
kubectl create namespace chat-platform
```

**Deploy PostgreSQL (StatefulSet):**
```yaml
# k8s/databases/postgres/statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: chat-platform
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          ports:
            - containerPort: 5432
          env:
            - name: POSTGRES_USER
              valueFrom:
                secretKeyRef:
                  name: postgres-secret
                  key: username
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: postgres-secret
                  key: password
          volumeMounts:
            - name: postgres-data
              mountPath: /var/lib/postgresql/data
          resources:
            requests:
              memory: "256Mi"
              cpu: "250m"
            limits:
              memory: "512Mi"
              cpu: "500m"
          livenessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            initialDelaySeconds: 5
            periodSeconds: 5
  volumeClaimTemplates:
    - metadata:
        name: postgres-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
```

**Deploy MongoDB and Redis similarly with their own manifests.**

### Phase 2: First Service -- user-service in Go (Day 3-5)

```go
// services/user-service/main.go
package main

import (
    "database/sql"
    "encoding/json"
    "log"
    "net/http"
    "os"

    _ "github.com/lib/pq"
)

type User struct {
    ID    string `json:"id"`
    Email string `json:"email"`
    Name  string `json:"name"`
}

var db *sql.DB

func main() {
    var err error
    dsn := os.Getenv("DATABASE_URL")
    db, err = sql.Open("postgres", dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    http.HandleFunc("/health", healthHandler)
    http.HandleFunc("/ready", readyHandler)
    http.HandleFunc("/api/v1/users", usersHandler)

    port := os.Getenv("PORT")
    if port == "" {
        port = "8001"
    }
    log.Printf("user-service listening on :%s", port)
    log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
    if err := db.Ping(); err != nil {
        w.WriteHeader(http.StatusServiceUnavailable)
        return
    }
    w.WriteHeader(http.StatusOK)
}

func usersHandler(w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case http.MethodGet:
        // list users
    case http.MethodPost:
        // create user
    default:
        w.WriteHeader(http.StatusMethodNotAllowed)
    }
}
```

```dockerfile
# services/user-service/Dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /user-service

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /user-service /user-service
EXPOSE 8001
CMD ["/user-service"]
```

**Kubernetes manifests for user-service:**
```yaml
# k8s/user-service/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: user-service
  namespace: chat-platform
  labels:
    app: user-service
    version: v1
spec:
  replicas: 2
  selector:
    matchLabels:
      app: user-service
  template:
    metadata:
      labels:
        app: user-service
        version: v1
    spec:
      containers:
        - name: user-service
          image: user-service:latest
          ports:
            - containerPort: 8001
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: user-service-secret
                  key: database-url
            - name: PORT
              value: "8001"
          resources:
            requests:
              memory: "64Mi"
              cpu: "100m"
            limits:
              memory: "128Mi"
              cpu: "250m"
          livenessProbe:
            httpGet:
              path: /health
              port: 8001
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /ready
              port: 8001
            initialDelaySeconds: 5
            periodSeconds: 5
---
# k8s/user-service/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: user-service
  namespace: chat-platform
spec:
  selector:
    app: user-service
  ports:
    - port: 8001
      targetPort: 8001
  type: ClusterIP
```

### Phase 3: Build Remaining Services (Day 6-14)

Build each service following the same pattern:
1. Write the application code with health/ready endpoints
2. Create a multi-stage Dockerfile
3. Write K8s manifests (Deployment, Service, ConfigMap, Secret)
4. Load the image into Kind: `kind load docker-image <image> --name chat-platform`

**Order of implementation:**
1. chat-service (TypeScript + MongoDB) -- Real-time messaging, WebSocket handling
2. ai-service (Python/FastAPI + Redis) -- LLM integration, prompt processing
3. history-service (Go + PostgreSQL) -- Conversation persistence, search
4. moderation-service (TypeScript + PostgreSQL) -- Content filtering, safety
5. analytics-service (Python + Redis + ClickHouse) -- Usage tracking, insights
6. websocket-gateway (Go + Redis) -- WebSocket connection management
7. api-bff (TypeScript) -- Aggregate calls, WebSocket + REST proxy

### Phase 4: Tilt for Development Loop (Day 15-16)

```python
# Tiltfile
# Database dependencies
k8s_yaml('k8s/databases/postgres/statefulset.yaml')
k8s_yaml('k8s/databases/postgres/service.yaml')
k8s_yaml('k8s/databases/mongodb/statefulset.yaml')
k8s_yaml('k8s/databases/mongodb/service.yaml')
k8s_yaml('k8s/databases/redis/deployment.yaml')
k8s_yaml('k8s/databases/redis/service.yaml')

# User Service (Go)
docker_build('user-service', './services/user-service')
k8s_yaml(['k8s/user-service/deployment.yaml', 'k8s/user-service/service.yaml'])
k8s_resource('user-service', port_forwards='8001:8001')

# Product Service (TypeScript)
docker_build('product-service', './services/product-service')
k8s_yaml(['k8s/product-service/deployment.yaml', 'k8s/product-service/service.yaml'])
k8s_resource('product-service', port_forwards='8002:8002')

# Repeat for each service...
```

### Phase 5: Helm Charts (Day 17-18)

```yaml
# helm/ecommerce/values.yaml
global:
  namespace: chat-platform
  imagePullPolicy: IfNotPresent

userService:
  replicas: 2
  image: user-service:latest
  port: 8001
  resources:
    requests:
      cpu: 100m
      memory: 64Mi
    limits:
      cpu: 250m
      memory: 128Mi

productService:
  replicas: 2
  image: product-service:latest
  port: 8002

# ... repeat for all services

postgres:
  enabled: true
  storage: 1Gi

mongodb:
  enabled: true
  storage: 1Gi

redis:
  enabled: true
```

### Phase 6: HPA (Day 19-20)

```yaml
# k8s/user-service/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: user-service-hpa
  namespace: chat-platform
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: user-service
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Pods
          value: 2
          periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Pods
          value: 1
          periodSeconds: 120
```

---

## Validation Checklist

- [ ] Kind cluster running with 3 nodes (1 control-plane, 2 workers)
- [ ] All databases deployed: PostgreSQL, MongoDB, Redis, ClickHouse
- [ ] Each service has health + readiness probes passing
- [ ] Each service connects to its own database (no shared DBs)
- [ ] Services written in at least 3 languages (Go, TypeScript, Python)
- [ ] All services reachable via ClusterIP Services
- [ ] WebSocket Gateway handles real-time connections properly
- [ ] API BFF aggregates data from multiple services in a single call
- [ ] AI Service integrates with OpenAI API or local LLM
- [ ] Tilt or Skaffold provides live-reload development
- [ ] Helm chart deploys the full stack with `helm install`
- [ ] HPA configured and scaling works under load
- [ ] `kubectl get pods -n chat-platform` shows all pods Running/Ready
- [ ] Can complete full chat flow: login -> send message -> AI response -> conversation history

---

## Key Concepts to Internalize

1. **Database-per-service**: each service owns its data. No service reads another's DB.
2. **Multi-stage Docker builds**: keep images small (alpine base, no build tools in final image)
3. **Health vs Readiness probes**: health = "am I alive?", readiness = "can I serve traffic?"
4. **Resource requests/limits**: requests = scheduling guarantee, limits = hard cap
5. **StatefulSet vs Deployment**: use StatefulSet for databases (stable network identity, ordered scaling)
6. **ConfigMaps vs Secrets**: configs for non-sensitive, secrets for sensitive (API keys, DB passwords)
7. **WebSocket vs HTTP**: WebSockets for real-time bidirectional communication, HTTP for standard APIs
8. **Async AI Processing**: never block on LLM calls; use async patterns and message queues
9. **Vector Storage**: semantic search requires vector databases for AI-powered features
10. **Rate Limiting**: essential for AI APIs to control costs and prevent abuse

---

## What You'll Carry Forward

Every subsequent project builds on this chat platform. Keep this running as you add:
- API Gateway with WebSocket support (Project 02)
- Full observability for chat flows (Project 03)
- Kafka events for real-time messaging (Project 04)
- CQRS for conversation history (Project 05)
- AI workflow orchestration (Project 06)
- And everything else...

This forms the foundation for a production-ready ChatGPT-like platform with enterprise microservices patterns.
