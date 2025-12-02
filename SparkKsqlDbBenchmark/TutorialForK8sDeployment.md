# Tutorial: Docker Registry → Kubernetes Deployment

---

## 📋 Table of Contents

1. [GitLab Container Registry Setup](#1-gitlab-container-registry-setup)
2. [Build & Push Docker Images](#2-build--push-docker-images)
3. [Create Kubernetes Secret](#3-create-kubernetes-secret)
4. [Update Deployment Files](#4-update-deployment-files)
5. [Deploy to Kubernetes](#5-deploy-to-kubernetes)
6. [Troubleshooting](#6-troubleshooting)

---

## 1️⃣ GitLab Container Registry Setup

### Step 1.1: Get Your GitLab Access Token

1. Go to GitLab: https://gitlab.sdu.dk
2. Click your profile icon (top right) → **Settings**
3. Left sidebar → **Access Tokens**
4. Create new token:
   - **Name**: `k8s-registry-access`
   - **Expiration date**: Set far in future
   - **Scopes**: Check `read_registry` and `write_registry`
5. Click **Create personal access token**
6. **SAVE THE TOKEN** - you won't see it again!

### Step 1.2: Login to GitLab Registry

```bash
# Login to GitLab Container Registry
docker login registry.gitlab.sdu.dk

# When prompted:
# Username: your-gitlab-username
# Password: paste-your-token-here
```

You should see: `Login Succeeded`

---

## 2️⃣ Build & Push Docker Images

### Step 2.1: Set Your Registry URL

```bash
# Set your registry path
export REGISTRY="registry.gitlab.sdu.dk/the-european-avengers/bigdataproject"

# Verify it's set
echo $REGISTRY
```

### Step 2.2: Build All Images for Linux AMD64

```bash
cd ~/path/to/SparkKsqlDbBenchmark

# Build Producer
docker buildx build \
  --platform linux/amd64 \
  -t $REGISTRY/benchmark-producer:latest \
  --push \
  producer/

# Build Spark Consumer
docker buildx build \
  --platform linux/amd64 \
  -t $REGISTRY/spark-consumer:latest \
  --push \
  spark-consumer/

# Build Latency Monitor
docker buildx build \
  --platform linux/amd64 \
  -t $REGISTRY/latency-monitor:latest \
  --push \
  latency-monitor/
```

### Step 2.3: Verify Images Were Pushed

```bash
# Check images exist
docker manifest inspect $REGISTRY/benchmark-producer:latest
docker manifest inspect $REGISTRY/spark-consumer:latest
docker manifest inspect $REGISTRY/latency-monitor:latest

# Or go to GitLab web UI:
# https://gitlab.sdu.dk/the-european-avengers/bigdataproject/container_registry
```

---

## 3️⃣ Create Kubernetes Secret

### Step 3.1: Create Docker Registry Secret

```bash
# Create secret for pulling images
kubectl create secret docker-registry gitlab-registry \
  --docker-server=registry.gitlab.sdu.dk \
  --docker-username=YOUR_GITLAB_USERNAME \
  --docker-password=YOUR_GITLAB_TOKEN \
  --docker-email=your-email@example.com \
  --namespace=bd-bd-gr-05

# Verify secret was created
kubectl get secret gitlab-registry -n bd-bd-gr-05
```

### Step 3.2: (Alternative) Create from Docker Config

If you already did `docker login`:

```bash
# This uses your existing docker credentials
kubectl create secret generic gitlab-registry \
  --from-file=.dockerconfigjson=$HOME/.docker/config.json \
  --type=kubernetes.io/dockerconfigjson \
  --namespace=bd-bd-gr-05
```

### Step 3.3: Verify Secret Contents

```bash
# View the secret (base64 encoded)
kubectl get secret gitlab-registry -n bd-bd-gr-05 -o yaml

# Decode to verify (optional)
kubectl get secret gitlab-registry -n bd-bd-gr-05 -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d
```

---

## 4️⃣ Update Deployment Files

### Step 4.1: Update deployment.yaml

Edit `k8s/deployment.yaml` to add `imagePullSecrets`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: bd-bd-gr-05

---
# Schema Registry Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-registry
  namespace: bd-bd-gr-05
spec:
  replicas: 1
  selector:
    matchLabels:
      app: schema-registry
  template:
    metadata:
      labels:
        app: schema-registry
    spec:
      # ADD THIS for Schema Registry (using public image)
      # imagePullSecrets: []  # Not needed for public Confluent images
      containers:
      - name: schema-registry
        image: confluentinc/cp-schema-registry:7.5.0
        ports:
        - containerPort: 8081
        env:
        - name: SCHEMA_REGISTRY_HOST_NAME
          value: "schema-registry"
        - name: SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS
          value: "kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-1.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-2.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092"
        - name: SCHEMA_REGISTRY_LISTENERS
          value: "http://0.0.0.0:8081"
        - name: SCHEMA_REGISTRY_KAFKASTORE_TOPIC_SKIP_VALIDATION
          value: "true"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1"

---
# Schema Registry Service
apiVersion: v1
kind: Service
metadata:
  name: schema-registry
  namespace: bd-bd-gr-05
spec:
  selector:
    app: schema-registry
  ports:
  - port: 8081
    targetPort: 8081

---
# Spark Consumer Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-consumer
  namespace: bd-bd-gr-05
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-consumer
  template:
    metadata:
      labels:
        app: spark-consumer
    spec:
      # ADD THIS - Pull from private registry
      imagePullSecrets:
      - name: gitlab-registry
      containers:
      - name: spark-consumer
        image: registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/spark-consumer:latest
        imagePullPolicy: Always  # Always pull latest
        args: ["100"]
        ports:
        - containerPort: 4040
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-1.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-2.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092"
        - name: SCHEMA_REGISTRY_URL
          value: "http://schema-registry:8081"
        - name: WINDOW_DURATION
          value: "1 minute"
        - name: TRIGGER_INTERVAL
          value: "2 seconds"
        - name: SHUFFLE_PARTITIONS
          value: "10"
        - name: MAX_OFFSETS_PER_TRIGGER
          value: "5000"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "6Gi"
            cpu: "4"

---
# Spark Consumer Service
apiVersion: v1
kind: Service
metadata:
  name: spark-consumer
  namespace: bd-bd-gr-05
spec:
  selector:
    app: spark-consumer
  ports:
  - port: 4040
    targetPort: 4040
```

### Step 4.2: Update jobs.yaml

Edit `k8s/jobs.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: benchmark-producer
  namespace: bd-bd-gr-05
spec:
  ttlSecondsAfterFinished: 300
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: producer
    spec:
      restartPolicy: Never
      # ADD THIS - Pull from private registry
      imagePullSecrets:
      - name: gitlab-registry
      containers:
      - name: producer
        image: registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/benchmark-producer:latest
        imagePullPolicy: Always  # Always pull latest
        args: ["100"]
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-1.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-2.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092"
        - name: SCHEMA_REGISTRY_URL
          value: "http://schema-registry:8081"
        resources:
          requests:
            memory: "1Gi"
            cpu: "1"
          limits:
            memory: "2Gi"
            cpu: "2"

---
apiVersion: batch/v1
kind: Job
metadata:
  name: latency-monitor
  namespace: bd-bd-gr-05
spec:
  ttlSecondsAfterFinished: 600
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: latency-monitor
    spec:
      restartPolicy: Never
      # ADD THIS - Pull from private registry
      imagePullSecrets:
      - name: gitlab-registry
      containers:
      - name: monitor
        image: registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/latency-monitor:latest
        imagePullPolicy: Always  # Always pull latest
        args: ["100"]
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-1.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092,kafka-g5-controller-2.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092"
        - name: SCHEMA_REGISTRY_URL
          value: "http://schema-registry:8081"
        - name: INPUT_TOPIC
          value: "weather.aggregated.output"
        - name: MAX_WAIT_TIME_MS
          value: "60000"
        - name: MAX_EMPTY_POLLS
          value: "10"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1"
```

---

## 5️⃣ Deploy to Kubernetes

### Step 5.1: Apply Deployment

```bash
# Apply deployment
kubectl apply -f k8s/deployment.yaml

# Watch pods start
kubectl get pods -n bd-bd-gr-05 -w
```

### Step 5.2: Verify Pods are Running

```bash
# Check pod status
kubectl get pods -n bd-bd-gr-05

# Should show:
# schema-registry-xxx    1/1   Running   0   1m
# spark-consumer-xxx     1/1   Running   0   1m
```

### Step 5.3: Run Full Benchmark

```bash
./run-k8s-benchmark.sh 100
```

---

## 6️⃣ Troubleshooting

### Issue 1: ImagePullBackOff

```bash
# Check pod details
kubectl describe pod -n bd-bd-gr-05 <pod-name>

# Common causes:
# - Wrong credentials in secret
# - Wrong image name
# - Image not pushed
# - Wrong architecture (not linux/amd64)
```

**Fix:**
```bash
# Recreate secret with correct credentials
kubectl delete secret gitlab-registry -n bd-bd-gr-05
kubectl create secret docker-registry gitlab-registry \
  --docker-server=registry.gitlab.sdu.dk \
  --docker-username=YOUR_USERNAME \
  --docker-password=YOUR_TOKEN \
  --namespace=bd-bd-gr-05

# Delete pod to restart
kubectl delete pod -n bd-bd-gr-05 <pod-name>
```

### Issue 2: ErrImagePull - Platform Mismatch

```bash
# Error: no match for platform in manifest
```

**Fix:**
```bash
# Rebuild for correct platform
docker buildx build \
  --platform linux/amd64 \
  -t $REGISTRY/your-image:latest \
  --push \
  your-directory/
```

### Issue 3: Secret Not Found

```bash
# Error: secret "gitlab-registry" not found
```

**Fix:**
```bash
# Make sure secret exists in correct namespace
kubectl get secrets -n bd-bd-gr-05 | grep gitlab

# If missing, create it
kubectl create secret docker-registry gitlab-registry \
  --docker-server=registry.gitlab.sdu.dk \
  --docker-username=YOUR_USERNAME \
  --docker-password=YOUR_TOKEN \
  --namespace=bd-bd-gr-05
```

### Issue 4: Test Image Pull Manually

```bash
# Create test pod
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: test-image-pull
  namespace: bd-bd-gr-05
spec:
  imagePullSecrets:
  - name: gitlab-registry
  containers:
  - name: test
    image: registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/spark-consumer:latest
    command: ["sleep", "3600"]
EOF

# Check if it works
kubectl get pod test-image-pull -n bd-bd-gr-05

# Clean up
kubectl delete pod test-image-pull -n bd-bd-gr-05
```

---

## 📝 Quick Reference Commands

```bash
# Build all images
for component in producer spark-consumer latency-monitor; do
  docker buildx build --platform linux/amd64 \
    -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/$component:latest \
    --push $component/
done

# Create secret
kubectl create secret docker-registry gitlab-registry \
  --docker-server=registry.gitlab.sdu.dk \
  --docker-username=YOUR_USERNAME \
  --docker-password=YOUR_TOKEN \
  --namespace=bd-bd-gr-05

# Deploy
kubectl apply -f k8s/deployment.yaml

# Run benchmark
./run-k8s-benchmark.sh 100

# Check status
kubectl get pods -n bd-bd-gr-05
kubectl logs -n bd-bd-gr-05 -l app=spark-consumer
```

---

## ✅ Checklist

- [ ] GitLab access token created
- [ ] Logged into Docker registry (`docker login`)
- [ ] All 3 images built for `linux/amd64`
- [ ] All 3 images pushed to GitLab registry
- [ ] Kubernetes secret created
- [ ] `deployment.yaml` updated with `imagePullSecrets`
- [ ] `jobs.yaml` updated with `imagePullSecrets`
- [ ] Deployments applied
- [ ] Pods running successfully
- [ ] Benchmark completed

Now follow these steps and let me know where you get stuck! 🚀