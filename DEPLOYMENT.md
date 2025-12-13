# Vultisig Analytics Dashboard - Deployment Guide

This document provides step-by-step instructions for deploying the Vultisig Analytics Dashboard with:
- **Frontend**: Next.js 16 on Vercel
- **Backend**: Python Flask API on Kubernetes

## Architecture Overview

```
                    +-------------------+
                    |    End Users      |
                    +--------+----------+
                             |
              +--------------+--------------+
              |                             |
    +---------v---------+         +---------v---------+
    |   Vercel CDN      |         |   K8s Ingress     |
    |   (Frontend)      |         |   (API Gateway)   |
    +--------+----------+         +---------+---------+
             |                              |
    +--------v----------+         +---------v---------+
    |   Next.js App     |-------->|   Flask API       |
    |   (SSG/CSR)       |  HTTPS  |   (Backend)       |
    +-------------------+         +---------+---------+
                                            |
                                  +---------v---------+
                                  |   PostgreSQL      |
                                  |   (TimescaleDB)   |
                                  +-------------------+
```

---

## Part 1: Frontend Deployment on Vercel

### Prerequisites

- GitHub, GitLab, or Bitbucket account
- Vercel account (free tier available)
- Backend API deployed and accessible

### Step 1: Connect Repository to Vercel

1. Go to [vercel.com](https://vercel.com) and sign in
2. Click "Add New..." -> "Project"
3. Import your Git repository
4. Vercel will auto-detect Next.js

### Step 2: Configure Project Settings

In the Vercel project settings, configure:

**Root Directory**: `dashboard`

This is critical since the Next.js app is in a subdirectory.

**Build & Development Settings**:
- Framework Preset: Next.js
- Build Command: `npm run build` (or leave default)
- Output Directory: `.next` (auto-detected)
- Install Command: `npm ci` (recommended)

### Step 3: Configure Environment Variables

Add the following environment variable in Vercel:

| Variable | Description | Example |
|----------|-------------|---------|
| `NEXT_PUBLIC_API_URL` | Backend API URL | `https://api.your-domain.com` |

**Important**:
- `NEXT_PUBLIC_` prefix makes the variable available to client-side code
- The value should be your production Kubernetes API endpoint
- No trailing slash

### Step 4: Deploy

1. Click "Deploy"
2. Vercel will:
   - Install dependencies
   - Build the Next.js app
   - Deploy to edge network

### Step 5: Configure Custom Domain (Optional)

1. Go to Project Settings -> Domains
2. Add your custom domain
3. Configure DNS records as instructed
4. SSL is automatically provisioned

### Vercel Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `NEXT_PUBLIC_API_URL` | Yes | Backend API base URL |
| `NEXT_PUBLIC_ENVIRONMENT` | No | Environment identifier (dev/staging/prod) |

---

## Part 2: Backend Deployment on Kubernetes

### Prerequisites

- Kubernetes cluster (EKS, GKE, AKS, or self-managed)
- kubectl configured with cluster access
- Container registry (Docker Hub, ECR, GCR, ACR)
- PostgreSQL/TimescaleDB database (managed or self-hosted)

### Step 1: Build and Push Docker Image

```bash
# Navigate to backend directory
cd vultisig-analytics

# Build the Docker image
docker build -t your-registry/vultisig-backend:latest .

# Tag with version for rollbacks
docker tag your-registry/vultisig-backend:latest your-registry/vultisig-backend:v1.0.0

# Push to registry
docker push your-registry/vultisig-backend:latest
docker push your-registry/vultisig-backend:v1.0.0
```

### Step 2: Create Kubernetes Secrets

**Option A: Using kubectl (simple)**
```bash
kubectl create namespace vultisig-analytics

kubectl create secret generic vultisig-backend-secrets \
  --namespace=vultisig-analytics \
  --from-literal=DATABASE_URL='postgresql://user:password@host:5432/dbname' \
  --from-literal=ONEINCH_API_KEY='your-key' \
  --from-literal=LIFI_API_KEY='your-key' \
  --from-literal=ARKHAM_API_KEY='your-key' \
  --from-literal=ETHERSCAN_API_KEY='your-key' \
  --from-literal=INFURA_API_KEY='your-key' \
  --from-literal=ALCHEMY_API_KEY='your-key' \
  --from-literal=MORALIS_API_KEY='your-key'
```

**Option B: Using sealed-secrets (recommended for GitOps)**
```bash
# Install sealed-secrets controller first
# Then encrypt your secrets
kubeseal --format yaml < k8s/secret.yaml > k8s/sealed-secret.yaml
```

**Option C: External Secrets Operator (enterprise)**
```yaml
# Example with AWS Secrets Manager
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: vultisig-backend-secrets
  namespace: vultisig-analytics
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-store
    kind: ClusterSecretStore
  target:
    name: vultisig-backend-secrets
  data:
    - secretKey: DATABASE_URL
      remoteRef:
        key: vultisig/backend
        property: database_url
```

### Step 3: Update Manifests

1. Edit `k8s/deployment.yaml`:
   - Update `image:` to your registry path

2. Edit `k8s/ingress.yaml`:
   - Update `host:` to your API domain
   - Update CORS `cors-allow-origin` to your Vercel domain
   - Update TLS secret name if using cert-manager

3. Edit `k8s/kustomization.yaml`:
   - Update image name and tag

### Step 4: Deploy to Kubernetes

**Using Kustomize (recommended)**:
```bash
# Apply all manifests
kubectl apply -k k8s/

# Or preview first
kubectl apply -k k8s/ --dry-run=client -o yaml
```

**Using kubectl directly**:
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml  # Only if not using kubectl create secret
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/ingress.yaml
kubectl apply -f k8s/hpa.yaml
kubectl apply -f k8s/pdb.yaml
kubectl apply -f k8s/network-policy.yaml
```

### Step 5: Verify Deployment

```bash
# Check pods are running
kubectl get pods -n vultisig-analytics

# Check service
kubectl get svc -n vultisig-analytics

# Check ingress
kubectl get ingress -n vultisig-analytics

# View logs
kubectl logs -n vultisig-analytics -l app.kubernetes.io/name=vultisig-analytics -f

# Test health endpoint
kubectl port-forward -n vultisig-analytics svc/vultisig-backend 8080:8080
curl http://localhost:8080/api/health
```

### Step 6: Configure TLS (if using cert-manager)

```bash
# Ensure cert-manager is installed
kubectl get pods -n cert-manager

# Create ClusterIssuer for Let's Encrypt
cat <<EOF | kubectl apply -f -
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: your-email@example.com
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
    - http01:
        ingress:
          class: nginx
EOF

# Uncomment the cert-manager annotation in k8s/ingress.yaml
# cert-manager.io/cluster-issuer: letsencrypt-prod
```

---

## Environment Variables Reference

### Frontend (Dashboard)

| Variable | Required | Build/Runtime | Description |
|----------|----------|---------------|-------------|
| `NEXT_PUBLIC_API_URL` | Yes | Build | Backend API base URL (e.g., `https://api.vultisig.com`) |

### Backend (API Server)

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `ONEINCH_API_KEY` | Yes | 1inch API key for swap data |
| `LIFI_API_KEY` | Yes | LiFi API key for cross-chain data |
| `ARKHAM_API_KEY` | Yes | Arkham Intelligence API key |
| `ETHERSCAN_API_KEY` | No | Etherscan API key |
| `INFURA_API_KEY` | No | Infura RPC API key |
| `ALCHEMY_API_KEY` | No | Alchemy RPC API key |
| `MORALIS_API_KEY` | No | Moralis API key |
| `FLASK_ENV` | No | Flask environment (production/development) |
| `LOG_LEVEL` | No | Logging level (INFO/DEBUG/WARNING/ERROR) |

---

## Troubleshooting

### Frontend (Vercel)

#### Build Fails
```
Error: Cannot find module 'X'
```
**Solution**: Check that all dependencies are in `package.json`, not `devDependencies` if needed at runtime.

#### API Calls Fail
```
TypeError: Failed to fetch
```
**Solution**:
1. Check `NEXT_PUBLIC_API_URL` is set correctly in Vercel
2. Verify CORS is configured on the backend Ingress
3. Check browser console for specific error

#### Wrong Root Directory
```
Error: Could not find a `next.config` file
```
**Solution**: Set "Root Directory" to `dashboard` in Vercel project settings.

### Backend (Kubernetes)

#### Pods Not Starting
```bash
kubectl describe pod -n vultisig-analytics <pod-name>
```
Common issues:
- Image pull errors: Check registry credentials
- OOMKilled: Increase memory limits
- CrashLoopBackOff: Check logs for application errors

#### Health Checks Failing
```bash
kubectl logs -n vultisig-analytics <pod-name>
```
Common issues:
- Database connection failed: Check `DATABASE_URL` secret
- Port mismatch: Verify containerPort matches service targetPort

#### Ingress Not Working
```bash
kubectl describe ingress -n vultisig-analytics vultisig-backend
```
Common issues:
- No address assigned: Check ingress controller is running
- TLS errors: Verify certificate secret exists
- 503 errors: Check service selectors match pod labels

#### Database Connection Issues
```bash
# Test from inside a pod
kubectl exec -n vultisig-analytics -it <pod-name> -- /bin/sh
# Then try connecting
python -c "import psycopg2; psycopg2.connect('$DATABASE_URL')"
```
Common issues:
- Network policies blocking egress
- Wrong connection string format
- Database not accessible from cluster

---

## Rollback Procedures

### Frontend (Vercel)

1. Go to Project -> Deployments
2. Find the previous working deployment
3. Click "..." -> "Promote to Production"

### Backend (Kubernetes)

```bash
# View rollout history
kubectl rollout history deployment/vultisig-backend -n vultisig-analytics

# Rollback to previous version
kubectl rollout undo deployment/vultisig-backend -n vultisig-analytics

# Rollback to specific revision
kubectl rollout undo deployment/vultisig-backend -n vultisig-analytics --to-revision=2

# Check rollout status
kubectl rollout status deployment/vultisig-backend -n vultisig-analytics
```

---

## Monitoring and Observability

### Recommended Setup

1. **Metrics**: Prometheus + Grafana
   - Add Prometheus annotations to deployment
   - Create Grafana dashboards for key metrics

2. **Logging**: ELK Stack or Loki
   - Collect container logs
   - Set up log-based alerts

3. **Tracing**: Jaeger or Zipkin
   - Add OpenTelemetry instrumentation
   - Trace cross-service requests

4. **Alerts**: PagerDuty or Opsgenie
   - Configure alerts for:
     - High error rate (>1%)
     - High latency (p99 > 2s)
     - Pod restarts
     - Database connection failures

---

## Security Checklist

- [ ] All secrets stored in Kubernetes Secrets or external vault
- [ ] TLS enabled on Ingress
- [ ] Network policies restricting traffic
- [ ] Pod security context (non-root, read-only filesystem)
- [ ] Regular security scanning of container images
- [ ] CORS properly configured (specific origins, not *)
- [ ] Rate limiting enabled on Ingress
- [ ] API authentication (if applicable)
