# UrbanPulse Kubernetes

This directory is a Kustomize base for running UrbanPulse in Kubernetes.

## Configure Secrets

Create the namespace and secret before applying the app:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl -n urbanpulse create secret generic urbanpulse-secrets \
  --from-literal=GOLEMIO_API_KEY='replace-me' \
  --from-literal=POSTGRES_PASSWORD='replace-me'
```

`secret.example.yaml` shows the expected keys. A local `secret.yaml` is ignored by git.

## Configure Images

The image placeholders in `kustomization.yaml` should match the images published by
GitHub Actions:

```bash
cd deploy/kubernetes
kustomize edit set image urbanpulse/frontend=ghcr.io/<owner>/<repo>/frontend:latest
kustomize edit set image urbanpulse/realtime-gateway=ghcr.io/<owner>/<repo>/realtime-gateway:latest
kustomize edit set image urbanpulse/data-service=ghcr.io/<owner>/<repo>/data-service:latest
```

Use a `sha-<commit>` tag instead of `latest` when you want repeatable deploys.

## Deploy

```bash
kubectl apply -k deploy/kubernetes
kubectl -n urbanpulse get pods
```

For local testing without ingress:

```bash
kubectl -n urbanpulse port-forward svc/frontend 5173:5173
kubectl -n urbanpulse port-forward svc/realtime-gateway 3000:3000
kubectl -n urbanpulse port-forward svc/data-api 8000:8000
```

For a real ingress, update `VITE_WS_URL` in `configmap.yaml` to the public WebSocket
URL, usually a `wss://.../ws` address.
