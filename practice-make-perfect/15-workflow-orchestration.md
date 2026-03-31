# Project 15: AI Training & Conversation Processing Workflows
# TIER 3 -- Expert Platform Engineering

## Goal
Build complex DAG-based data pipelines and a Kubernetes-native CI/CD
pipeline using Argo Workflows. Each step runs as a K8s pod with its own
container image, with retry logic, timeouts, conditional branching, and
artifact passing.

---

## Architecture

```
  [Argo Workflows Controller]
         |
  [Workflow Template]
         |
  DAG:
  [ingest] --> [validate] --> [transform] --> [load] --> [notify]
                    |                           |
              [on-failure: alert]        [on-failure: rollback]

  CI/CD:
  [lint] --> [test] --> [build] --> [scan] --> [deploy via ArgoCD]
```

---

## Directory Structure

```
15-workflow-orchestration/
├── argo-workflows/
│   ├── install.sh
│   └── workflow-controller-configmap.yaml
├── data-pipelines/
│   ├── templates/
│   │   ├── ingest-template.yaml
│   │   ├── validate-template.yaml
│   │   ├── transform-template.yaml
│   │   ├── load-template.yaml
│   │   └── notify-template.yaml
│   ├── workflows/
│   │   ├── daily-etl.yaml
│   │   ├── order-analytics.yaml
│   │   └── data-quality-check.yaml
│   └── cron-workflows/
│       └── nightly-etl.yaml
├── ci-cd/
│   ├── templates/
│   │   ├── lint-template.yaml
│   │   ├── test-template.yaml
│   │   ├── build-template.yaml
│   │   ├── security-scan-template.yaml
│   │   └── deploy-template.yaml
│   ├── workflows/
│   │   └── service-ci-cd.yaml
│   └── event-sources/
│       └── github-webhook.yaml
├── sensors/
│   ├── ci-trigger.yaml
│   └── pipeline-trigger.yaml
├── containers/
│   ├── ingest/
│   │   ├── Dockerfile
│   │   └── main.py
│   ├── transform/
│   │   ├── Dockerfile
│   │   └── main.py
│   └── data-quality/
│       ├── Dockerfile
│       └── main.py
└── monitoring/
    └── workflow-dashboard.json
```

---

## Step-by-Step Implementation

### Phase 1: Install Argo Workflows (Day 1)

```bash
# Install Argo Workflows
kubectl create namespace argo
kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/latest/download/install.yaml

# Patch auth mode for local development
kubectl patch deployment argo-server -n argo \
  --type='json' \
  -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": ["server", "--auth-mode=server"]}]'

# Port forward UI
kubectl port-forward svc/argo-server -n argo 2746:2746

# Install CLI
curl -sLO https://github.com/argoproj/argo-workflows/releases/latest/download/argo-linux-amd64.gz
gunzip argo-linux-amd64.gz
chmod +x argo-linux-amd64 && sudo mv argo-linux-amd64 /usr/local/bin/argo

# Configure default artifact storage (S3-compatible with MinIO)
helm install minio oci://registry-1.docker.io/bitnamicharts/minio \
  -n argo \
  --set auth.rootUser=admin \
  --set auth.rootPassword=password123 \
  --set defaultBuckets=argo-artifacts
```

```yaml
# argo-workflows/workflow-controller-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: workflow-controller-configmap
  namespace: argo
data:
  artifactRepository: |
    s3:
      bucket: argo-artifacts
      endpoint: minio.argo:9000
      insecure: true
      accessKeySecret:
        name: minio-creds
        key: accesskey
      secretKeySecret:
        name: minio-creds
        key: secretkey
```

### Phase 2: Data Pipeline -- DAG Workflow (Day 2-5)

```yaml
# data-pipelines/workflows/order-analytics.yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: order-analytics-
  namespace: argo
spec:
  entrypoint: analytics-pipeline
  arguments:
    parameters:
      - name: date
        value: "2026-03-23"
      - name: source-db
        value: "postgres://orderdb.ecommerce:5432/orderdb"

  # Volumes for sharing data between steps
  volumes:
    - name: workspace
      emptyDir: {}

  templates:
    - name: analytics-pipeline
      dag:
        tasks:
          # Step 1: Extract orders from database
          - name: ingest-orders
            template: ingest
            arguments:
              parameters:
                - name: query
                  value: "SELECT * FROM orders WHERE created_at::date = '{{workflow.parameters.date}}'"
                - name: output-file
                  value: "/workspace/raw-orders.json"

          # Step 2: Extract products (parallel with orders)
          - name: ingest-products
            template: ingest
            arguments:
              parameters:
                - name: query
                  value: "SELECT * FROM products"
                - name: output-file
                  value: "/workspace/raw-products.json"

          # Step 3: Validate data (depends on both ingests)
          - name: validate
            template: validate-data
            dependencies: [ingest-orders, ingest-products]
            arguments:
              artifacts:
                - name: orders
                  from: "{{tasks.ingest-orders.outputs.artifacts.data}}"
                - name: products
                  from: "{{tasks.ingest-products.outputs.artifacts.data}}"

          # Step 4: Transform (depends on validation)
          - name: transform
            template: transform-data
            dependencies: [validate]
            arguments:
              artifacts:
                - name: validated-orders
                  from: "{{tasks.validate.outputs.artifacts.validated}}"

          # Step 5: Load to analytics store (depends on transform)
          - name: load-elasticsearch
            template: load-es
            dependencies: [transform]
            arguments:
              artifacts:
                - name: transformed
                  from: "{{tasks.transform.outputs.artifacts.transformed}}"

          # Step 6: Update Redis dashboard cache (parallel with ES load)
          - name: load-redis
            template: load-redis-cache
            dependencies: [transform]
            arguments:
              artifacts:
                - name: transformed
                  from: "{{tasks.transform.outputs.artifacts.transformed}}"

          # Step 7: Notify on completion (depends on both loads)
          - name: notify
            template: send-notification
            dependencies: [load-elasticsearch, load-redis]
            arguments:
              parameters:
                - name: message
                  value: "Analytics pipeline for {{workflow.parameters.date}} completed"

    # --- Template definitions ---

    - name: ingest
      inputs:
        parameters:
          - name: query
          - name: output-file
      container:
        image: ingest-worker:latest
        command: [python, /app/main.py]
        args:
          - "--query={{inputs.parameters.query}}"
          - "--output={{inputs.parameters.output-file}}"
          - "--db-url={{workflow.parameters.source-db}}"
        resources:
          requests:
            cpu: 200m
            memory: 256Mi
      outputs:
        artifacts:
          - name: data
            path: "{{inputs.parameters.output-file}}"

    - name: validate-data
      inputs:
        artifacts:
          - name: orders
            path: /workspace/orders.json
          - name: products
            path: /workspace/products.json
      container:
        image: data-quality:latest
        command: [python, /app/main.py]
        args:
          - "--orders=/workspace/orders.json"
          - "--products=/workspace/products.json"
          - "--output=/workspace/validated.json"
      outputs:
        artifacts:
          - name: validated
            path: /workspace/validated.json
      # Retry on transient failures
      retryStrategy:
        limit: 3
        retryPolicy: "Always"
        backoff:
          duration: "10s"
          factor: 2
          maxDuration: "1m"

    - name: transform-data
      inputs:
        artifacts:
          - name: validated-orders
            path: /workspace/input.json
      container:
        image: transform-worker:latest
        command: [python, /app/main.py]
        args:
          - "--input=/workspace/input.json"
          - "--output=/workspace/transformed.json"
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 1
            memory: 2Gi
      outputs:
        artifacts:
          - name: transformed
            path: /workspace/transformed.json
      # Timeout: if transform takes > 10 min, something is wrong
      activeDeadlineSeconds: 600

    - name: load-es
      inputs:
        artifacts:
          - name: transformed
            path: /workspace/data.json
      container:
        image: es-loader:latest
        command: [python, /app/main.py]
        args:
          - "--input=/workspace/data.json"
          - "--es-url=http://elasticsearch.observability:9200"
          - "--index=order-analytics"

    - name: load-redis-cache
      inputs:
        artifacts:
          - name: transformed
            path: /workspace/data.json
      container:
        image: redis-loader:latest
        command: [python, /app/main.py]
        args:
          - "--input=/workspace/data.json"
          - "--redis-url=redis://redis.ecommerce:6379"

    - name: send-notification
      inputs:
        parameters:
          - name: message
      container:
        image: curlimages/curl:latest
        command: [sh, -c]
        args:
          - |
            curl -X POST http://notification-service.ecommerce:8006/api/v1/notify \
              -H "Content-Type: application/json" \
              -d '{"channel": "pipeline-alerts", "message": "{{inputs.parameters.message}}"}'
```

### Phase 3: Cron Workflow (Day 5)

```yaml
# data-pipelines/cron-workflows/nightly-etl.yaml
apiVersion: argoproj.io/v1alpha1
kind: CronWorkflow
metadata:
  name: nightly-order-analytics
  namespace: argo
spec:
  schedule: "0 2 * * *"              # Every night at 2 AM
  timezone: "America/New_York"
  concurrencyPolicy: "Forbid"        # Don't run if previous still running
  startingDeadlineSeconds: 600        # Must start within 10 min of schedule
  successfulJobsHistoryLimit: 7       # Keep 7 days of history
  failedJobsHistoryLimit: 3
  workflowSpec:
    entrypoint: analytics-pipeline
    arguments:
      parameters:
        - name: date
          value: "{{workflow.scheduledTime.Format \"2006-01-02\"}}"
    # ... same templates as above
```

### Phase 4: CI/CD Pipeline (Day 6-9)

```yaml
# ci-cd/workflows/service-ci-cd.yaml
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: service-ci-cd
  namespace: argo
spec:
  arguments:
    parameters:
      - name: repo-url
      - name: branch
        value: main
      - name: service-name
      - name: image-registry
        value: "localhost:5000"

  entrypoint: ci-cd-pipeline

  templates:
    - name: ci-cd-pipeline
      dag:
        tasks:
          - name: checkout
            template: git-checkout

          - name: lint
            template: lint-code
            dependencies: [checkout]
            arguments:
              artifacts:
                - name: source
                  from: "{{tasks.checkout.outputs.artifacts.source}}"

          - name: unit-test
            template: run-tests
            dependencies: [checkout]
            arguments:
              artifacts:
                - name: source
                  from: "{{tasks.checkout.outputs.artifacts.source}}"

          - name: build-image
            template: build-and-push
            dependencies: [lint, unit-test]
            arguments:
              artifacts:
                - name: source
                  from: "{{tasks.checkout.outputs.artifacts.source}}"

          - name: security-scan
            template: scan-image
            dependencies: [build-image]
            arguments:
              parameters:
                - name: image
                  value: "{{tasks.build-image.outputs.parameters.image-tag}}"

          - name: integration-test
            template: run-integration-tests
            dependencies: [build-image]
            arguments:
              parameters:
                - name: image
                  value: "{{tasks.build-image.outputs.parameters.image-tag}}"

          - name: deploy
            template: deploy-to-cluster
            dependencies: [security-scan, integration-test]
            # Only deploy if on main branch
            when: "'{{workflow.parameters.branch}}' == 'main'"
            arguments:
              parameters:
                - name: image
                  value: "{{tasks.build-image.outputs.parameters.image-tag}}"

    - name: git-checkout
      container:
        image: alpine/git:latest
        command: [sh, -c]
        args:
          - |
            git clone --branch {{workflow.parameters.branch}} \
              {{workflow.parameters.repo-url}} /workspace/source
      outputs:
        artifacts:
          - name: source
            path: /workspace/source

    - name: lint-code
      inputs:
        artifacts:
          - name: source
            path: /workspace/source
      container:
        image: golangci/golangci-lint:latest
        workingDir: /workspace/source
        command: [golangci-lint, run, ./...]
      retryStrategy:
        limit: 2

    - name: run-tests
      inputs:
        artifacts:
          - name: source
            path: /workspace/source
      container:
        image: golang:1.22
        workingDir: /workspace/source
        command: [go, test, -v, -race, -coverprofile=coverage.out, ./...]
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
      outputs:
        artifacts:
          - name: coverage
            path: /workspace/source/coverage.out

    - name: build-and-push
      inputs:
        artifacts:
          - name: source
            path: /workspace/source
      container:
        image: gcr.io/kaniko-project/executor:latest
        args:
          - "--context=/workspace/source"
          - "--dockerfile=/workspace/source/Dockerfile"
          - "--destination={{workflow.parameters.image-registry}}/{{workflow.parameters.service-name}}:{{workflow.uid}}"
          - "--cache=true"
      outputs:
        parameters:
          - name: image-tag
            value: "{{workflow.parameters.image-registry}}/{{workflow.parameters.service-name}}:{{workflow.uid}}"

    - name: scan-image
      inputs:
        parameters:
          - name: image
      container:
        image: aquasec/trivy:latest
        command: [trivy, image]
        args:
          - "--severity=HIGH,CRITICAL"
          - "--exit-code=1"            # Fail on HIGH/CRITICAL vulns
          - "{{inputs.parameters.image}}"

    - name: run-integration-tests
      inputs:
        parameters:
          - name: image
      # Deploy to temp namespace, run tests, cleanup
      container:
        image: bitnami/kubectl:latest
        command: [sh, -c]
        args:
          - |
            # Create temp namespace
            kubectl create namespace test-{{workflow.uid}}
            # Deploy service with test image
            kubectl run test-svc -n test-{{workflow.uid}} --image={{inputs.parameters.image}}
            # Run integration tests
            kubectl run test-runner -n test-{{workflow.uid}} --image=test-runner:latest \
              --env="TARGET=test-svc" -- /run-tests.sh
            # Wait and get result
            kubectl wait --for=condition=complete job/test-runner -n test-{{workflow.uid}} --timeout=300s
            # Cleanup
            kubectl delete namespace test-{{workflow.uid}}
      activeDeadlineSeconds: 600

    - name: deploy-to-cluster
      inputs:
        parameters:
          - name: image
      container:
        image: argoproj/argocd:latest
        command: [sh, -c]
        args:
          - |
            argocd app set {{workflow.parameters.service-name}} \
              --kustomize-image {{inputs.parameters.image}} \
              --server argocd-server.argocd
            argocd app sync {{workflow.parameters.service-name}} \
              --server argocd-server.argocd
            argocd app wait {{workflow.parameters.service-name}} \
              --server argocd-server.argocd --timeout 300
```

### Phase 5: Event-Triggered Workflows (Day 10-11)

```yaml
# sensors/ci-trigger.yaml
# Argo Events: trigger CI/CD on git push
apiVersion: argoproj.io/v1alpha1
kind: Sensor
metadata:
  name: ci-trigger
  namespace: argo
spec:
  dependencies:
    - name: github-push
      eventSourceName: github
      eventName: push
  triggers:
    - template:
        name: trigger-ci-cd
        argoWorkflow:
          operation: submit
          source:
            resource:
              apiVersion: argoproj.io/v1alpha1
              kind: Workflow
              metadata:
                generateName: ci-cd-
              spec:
                workflowTemplateRef:
                  name: service-ci-cd
                arguments:
                  parameters:
                    - name: repo-url
                      value: ""          # Filled from event
                    - name: branch
                      value: ""
                    - name: service-name
                      value: ""
          parameters:
            - src:
                dependencyName: github-push
                dataKey: body.repository.clone_url
              dest: spec.arguments.parameters.0.value
            - src:
                dependencyName: github-push
                dataKey: body.ref
              dest: spec.arguments.parameters.1.value
```

### Phase 6: Workflow of Workflows (Day 12)

```yaml
# Compose multiple workflows together
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: platform-release-
spec:
  entrypoint: release-all
  templates:
    - name: release-all
      dag:
        tasks:
          # Build and deploy all services in dependency order
          - name: deploy-user-service
            templateRef:
              name: service-ci-cd
              template: ci-cd-pipeline
            arguments:
              parameters:
                - name: service-name
                  value: user-service

          - name: deploy-product-service
            templateRef:
              name: service-ci-cd
              template: ci-cd-pipeline
            arguments:
              parameters:
                - name: service-name
                  value: product-service

          - name: deploy-order-service
            templateRef:
              name: service-ci-cd
              template: ci-cd-pipeline
            dependencies: [deploy-user-service, deploy-product-service]
            arguments:
              parameters:
                - name: service-name
                  value: order-service

          # Run analytics pipeline after all services deployed
          - name: run-analytics
            templateRef:
              name: order-analytics
              template: analytics-pipeline
            dependencies: [deploy-order-service]
```

---

## Validation Checklist

- [ ] Argo Workflows installed with UI accessible
- [ ] MinIO configured for artifact storage
- [ ] Data pipeline DAG: ingest -> validate -> transform -> load -> notify
- [ ] Parallel steps run simultaneously (ingest-orders || ingest-products)
- [ ] Artifacts passed between steps via S3
- [ ] Retry strategy works (fail a step, watch retry with backoff)
- [ ] Timeout kills long-running steps (activeDeadlineSeconds)
- [ ] Conditional execution: deploy only on main branch
- [ ] CronWorkflow triggers nightly ETL
- [ ] CI/CD pipeline: lint -> test -> build -> scan -> deploy
- [ ] Kaniko builds Docker images without Docker daemon
- [ ] Trivy security scan blocks images with critical vulnerabilities
- [ ] Integration tests run in ephemeral namespace
- [ ] ArgoCD deploy triggered from workflow step
- [ ] Event trigger: git push -> CI/CD workflow starts
- [ ] Workflow of Workflows: platform release orchestrates all services

---

## Key Concepts to Internalize

1. **DAG-based execution**: model pipelines as directed acyclic graphs. Parallel where possible, sequential where needed.
2. **Each step = a pod**: isolated execution environment. Any language, any tool.
3. **Artifacts**: pass data between steps via S3. Not volume mounts (those don't scale).
4. **WorkflowTemplates**: reusable, parameterized workflow definitions. Write once, use everywhere.
5. **CronWorkflows**: scheduled data pipelines that Kubernetes manages for you.
6. **Kubernetes-native CI/CD**: no external CI server needed. The cluster IS the CI/CD platform.
7. **Kaniko**: build Docker images inside Kubernetes without Docker-in-Docker or privileged containers.
