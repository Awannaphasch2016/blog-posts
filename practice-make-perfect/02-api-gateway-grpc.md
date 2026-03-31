# Project 02: API Gateway + gRPC with WebSocket Support
# TIER 1 -- Foundation

## Goal
Add an API gateway for external traffic management with WebSocket support
for real-time chat, and convert internal service-to-service communication
from REST to gRPC with Protocol Buffers for AI and chat services.

---

## Architecture

```
  Chat Client (browser/mobile)
          |
    [Kong Gateway] <-- REST + WebSocket, JWT auth, rate limiting
          |                    |
          |              WebSocket Upgrade
          |                    |
    +-----+-----+        [WebSocket Gateway]
    |           |               |
  [API BFF]  [Chat UI]    Real-time events
    |           |               |
    +-----gRPC internally-------+
          |           |         |
    [user-svc] [ai-svc] [chat-svc] [history-svc]
```

---

## Directory Structure

```
02-api-gateway-grpc-websocket/
├── proto/                          # Shared protobuf definitions
│   ├── user/v1/user.proto
│   ├── chat/v1/chat.proto
│   ├── ai/v1/ai.proto
│   ├── history/v1/history.proto
│   ├── moderation/v1/moderation.proto
│   └── common/v1/common.proto
├── gateway/
│   ├── kong/
│   │   ├── kong-values.yaml       # Helm values for Kong
│   │   ├── consumers.yaml         # API consumers (JWT)
│   │   ├── plugins/
│   │   │   ├── rate-limiting.yaml
│   │   │   ├── jwt-auth.yaml
│   │   │   ├── cors.yaml
│   │   │   ├── websocket-proxy.yaml
│   │   │   └── request-transformer.yaml
│   │   └── ingress/
│   │       ├── user-routes.yaml
│   │       ├── chat-routes.yaml
│   │       ├── ai-routes.yaml
│   │       ├── websocket-routes.yaml
│   │       └── history-routes.yaml
│   └── kong-dbless.yaml           # Declarative config alternative
├── services/
│   ├── user-service/
│   │   ├── grpc_server.go         # gRPC server implementation
│   │   ├── rest_handler.go        # REST handler (gateway-facing)
│   │   └── gen/                   # Generated protobuf code
│   ├── chat-service/
│   │   ├── grpc_server.ts
│   │   ├── websocket_handler.ts   # WebSocket message handling
│   │   └── gen/
│   ├── ai-service/
│   │   ├── grpc_server.py
│   │   ├── openai_client.py       # AI service integration
│   │   ├── grpc_clients.py        # Clients calling other services
│   │   └── gen/
│   ├── history-service/
│   │   ├── grpc_server.go
│   │   ├── grpc_clients.go
│   │   └── gen/
│   ├── websocket-gateway/
│   │   ├── websocket_server.go    # WebSocket connection management
│   │   ├── grpc_clients.go
│   │   └── gen/
│   └── api-bff/
│       ├── grpc_server.ts
│       ├── websocket_proxy.ts     # WebSocket proxy
│       └── gen/
├── k8s/
│   └── kong/
│       └── ingress-class.yaml
└── buf.yaml                       # Buf for proto management
```

---

## Step-by-Step Implementation

### Phase 1: Protocol Buffers + Code Generation (Day 1-2)

```protobuf
// proto/chat/v1/chat.proto
syntax = "proto3";
package chat.v1;
option go_package = "gen/chat/v1;chatv1";

message ChatMessage {
  string id = 1;
  string conversation_id = 2;
  string user_id = 3;
  string content = 4;
  MessageType type = 5;
  string created_at = 6;
}

enum MessageType {
  MESSAGE_TYPE_UNSPECIFIED = 0;
  MESSAGE_TYPE_USER = 1;
  MESSAGE_TYPE_AI = 2;
  MESSAGE_TYPE_SYSTEM = 3;
}

message SendMessageRequest {
  string id = 1;
}

message GetUserResponse {
  User user = 1;
}

message CreateUserRequest {
  string email = 1;
  string name = 2;
  string password = 3;
}

message CreateUserResponse {
  User user = 1;
}

message ListUsersRequest {
  int32 page_size = 1;
  string page_token = 2;
}

message ListUsersResponse {
  repeated User users = 1;
  string next_page_token = 2;
}

service UserService {
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  rpc CreateUser(CreateUserRequest) returns (CreateUserResponse);
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
}
```

```protobuf
// proto/order/v1/order.proto
syntax = "proto3";
package order.v1;
option go_package = "gen/order/v1;orderv1";

import "user/v1/user.proto";
import "product/v1/product.proto";

message Order {
  string id = 1;
  string user_id = 2;
  repeated OrderItem items = 3;
  OrderStatus status = 4;
  string created_at = 5;
  int64 total_cents = 6;
}

message OrderItem {
  string product_id = 1;
  int32 quantity = 2;
  int64 price_cents = 3;
}

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING = 1;
  ORDER_STATUS_CONFIRMED = 2;
  ORDER_STATUS_PAID = 3;
  ORDER_STATUS_SHIPPED = 4;
  ORDER_STATUS_DELIVERED = 5;
  ORDER_STATUS_CANCELLED = 6;
}

message CreateOrderRequest {
  string user_id = 1;
  repeated OrderItem items = 2;
}

message CreateOrderResponse {
  Order order = 1;
}

message GetOrderRequest {
  string id = 1;
}

message GetOrderResponse {
  Order order = 1;
}

service OrderService {
  rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse);
  rpc GetOrder(GetOrderRequest) returns (GetOrderResponse);
}
```

**Install Buf for proto management:**
```bash
# Install buf
curl -sSL https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64 -o buf
chmod +x buf && sudo mv buf /usr/local/bin/

# buf.yaml at project root
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE

# Generate code
# buf.gen.yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: services/gen
    opt: paths=source_relative
  - remote: buf.build/grpc/go
    out: services/gen
    opt: paths=source_relative

buf generate
```

### Phase 2: gRPC Server Implementation (Day 3-5)

**Go gRPC server (user-service):**
```go
// services/user-service/grpc_server.go
package main

import (
    "context"
    "net"
    "log"

    "google.golang.org/grpc"
    "google.golang.org/grpc/health"
    "google.golang.org/grpc/health/grpc_health_v1"
    "google.golang.org/grpc/reflection"

    userv1 "user-service/gen/user/v1"
)

type userServer struct {
    userv1.UnimplementedUserServiceServer
    repo *UserRepository
}

func (s *userServer) GetUser(ctx context.Context, req *userv1.GetUserRequest) (*userv1.GetUserResponse, error) {
    user, err := s.repo.FindByID(ctx, req.Id)
    if err != nil {
        return nil, err
    }
    return &userv1.GetUserResponse{User: user.ToProto()}, nil
}

func (s *userServer) CreateUser(ctx context.Context, req *userv1.CreateUserRequest) (*userv1.CreateUserResponse, error) {
    user, err := s.repo.Create(ctx, req.Email, req.Name, req.Password)
    if err != nil {
        return nil, err
    }
    return &userv1.CreateUserResponse{User: user.ToProto()}, nil
}

func startGRPCServer(repo *UserRepository) {
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()

    // Register service
    userv1.RegisterUserServiceServer(grpcServer, &userServer{repo: repo})

    // Health check for K8s
    healthServer := health.NewServer()
    grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
    healthServer.SetServingStatus("user.v1.UserService", grpc_health_v1.HealthCheckResponse_SERVING)

    // Reflection for debugging with grpcurl
    reflection.Register(grpcServer)

    log.Println("gRPC server listening on :50051")
    log.Fatal(grpcServer.Serve(lis))
}
```

**gRPC client (order-service calling user-service):**
```go
// services/order-service/grpc_clients.go
package main

import (
    "context"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    userv1 "order-service/gen/user/v1"
)

type ServiceClients struct {
    userClient userv1.UserServiceClient
    conn       *grpc.ClientConn
}

func NewServiceClients() (*ServiceClients, error) {
    // In K8s, use the service DNS name
    userConn, err := grpc.NewClient(
        "user-service.ecommerce.svc.cluster.local:50051",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        return nil, err
    }

    return &ServiceClients{
        userClient: userv1.NewUserServiceClient(userConn),
        conn:       userConn,
    }, nil
}

func (c *ServiceClients) GetUser(ctx context.Context, userID string) (*userv1.User, error) {
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    resp, err := c.userClient.GetUser(ctx, &userv1.GetUserRequest{Id: userID})
    if err != nil {
        return nil, err
    }
    return resp.User, nil
}
```

**Update K8s service to expose gRPC port:**
```yaml
# k8s/user-service/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: user-service
  namespace: ecommerce
spec:
  selector:
    app: user-service
  ports:
    - name: http
      port: 8001
      targetPort: 8001
    - name: grpc
      port: 50051
      targetPort: 50051
```

### Phase 3: Kong API Gateway (Day 6-8)

```bash
# Install Kong via Helm
helm repo add kong https://charts.konghq.com
helm repo update
helm install kong kong/ingress -n kong --create-namespace \
  --set gateway.proxy.type=NodePort \
  --set gateway.proxy.http.nodePort=30080
```

**Define routes via Ingress resources:**
```yaml
# gateway/kong/ingress/user-routes.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: user-service-routes
  namespace: ecommerce
  annotations:
    konghq.com/strip-path: "true"
    konghq.com/plugins: rate-limiting-plugin,jwt-auth-plugin,cors-plugin
spec:
  ingressClassName: kong
  rules:
    - http:
        paths:
          - path: /api/v1/users
            pathType: Prefix
            backend:
              service:
                name: user-service
                port:
                  number: 8001
```

**Rate limiting plugin:**
```yaml
# gateway/kong/plugins/rate-limiting.yaml
apiVersion: configuration.konghq.com/v1
kind: KongPlugin
metadata:
  name: rate-limiting-plugin
  namespace: ecommerce
config:
  minute: 100
  hour: 1000
  policy: local
  fault_tolerant: true
  hide_client_headers: false
plugin: rate-limiting
```

**JWT authentication plugin:**
```yaml
# gateway/kong/plugins/jwt-auth.yaml
apiVersion: configuration.konghq.com/v1
kind: KongPlugin
metadata:
  name: jwt-auth-plugin
  namespace: ecommerce
config:
  key_claim_name: kid
  claims_to_verify:
    - exp
plugin: jwt
```

**CORS plugin:**
```yaml
# gateway/kong/plugins/cors.yaml
apiVersion: configuration.konghq.com/v1
kind: KongPlugin
metadata:
  name: cors-plugin
  namespace: ecommerce
config:
  origins:
    - "http://localhost:3000"
  methods:
    - GET
    - POST
    - PUT
    - DELETE
    - OPTIONS
  headers:
    - Authorization
    - Content-Type
  credentials: true
  max_age: 3600
plugin: cors
```

### Phase 4: Request Transformation (Day 9-10)

**Transform headers -- pass user identity from JWT to upstream:**
```yaml
# gateway/kong/plugins/request-transformer.yaml
apiVersion: configuration.konghq.com/v1
kind: KongPlugin
metadata:
  name: request-transformer-plugin
  namespace: ecommerce
config:
  add:
    headers:
      - "X-User-ID:$(jwt.claim.sub)"
      - "X-Request-ID:$(request_id)"
plugin: request-transformer
```

### Phase 5: gRPC Probes in K8s (Day 11)

```yaml
# For gRPC services, use grpc health probes
containers:
  - name: user-service
    image: user-service:latest
    ports:
      - containerPort: 8001
        name: http
      - containerPort: 50051
        name: grpc
    livenessProbe:
      grpc:
        port: 50051
      initialDelaySeconds: 10
      periodSeconds: 15
    readinessProbe:
      grpc:
        port: 50051
      initialDelaySeconds: 5
      periodSeconds: 5
```

---

## Validation Checklist

- [ ] All proto files compile with `buf lint` passing
- [ ] gRPC code generated for Go and TypeScript
- [ ] Each service exposes both REST (for gateway) and gRPC (for internal) ports
- [ ] `grpcurl` can call services: `grpcurl -plaintext localhost:50051 user.v1.UserService/GetUser`
- [ ] Kong gateway routes external REST traffic to correct services
- [ ] JWT auth blocks unauthenticated requests at the gateway
- [ ] Rate limiting returns 429 when exceeded
- [ ] CORS headers present in responses
- [ ] order-service calls user-service via gRPC to validate user exists
- [ ] order-service calls product-service via gRPC to get product prices
- [ ] Request headers (X-User-ID, X-Request-ID) propagated to upstream services
- [ ] gRPC health probes working in K8s

---

## Key Concepts to Internalize

1. **API Gateway pattern**: single entry point handles cross-cutting concerns (auth, rate limiting, CORS, logging)
2. **Protocol Buffers**: strongly typed, backward-compatible schema evolution (never reuse field numbers)
3. **gRPC vs REST**: gRPC for internal (fast, typed, streaming), REST for external (browser-friendly, cacheable)
4. **Service DNS in K8s**: `<service>.<namespace>.svc.cluster.local:<port>`
5. **Kong CRDs**: KongPlugin, KongConsumer, Ingress annotations = declarative gateway config
6. **BFF pattern**: Backend-for-Frontend aggregates multiple service calls into one client-optimized response
