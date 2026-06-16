# UrbanPulse Kubernetes

This directory contains a Kustomize base for running UrbanPulse in Kubernetes.
It is still a learning-oriented deployment target, but the manifests have been
kept close to the current service layout so they can grow toward a real cluster
deployment.

## What Has Been Built

Manifests have been added for the namespace, shared configuration, secrets,
TimescaleDB, Redis, the FastAPI data service, the realtime gateway, and the
frontend. Image placeholders are kept in `kustomization.yaml` so published
GitHub Container Registry images can be substituted without editing every
manifest by hand.

`secret.example.yaml` documents the keys expected by the services. A local
`secret.yaml` is ignored by git so real credentials do not need to be committed.

## How It Works

The app expects an `urbanpulse` namespace and a secret named
`urbanpulse-secrets`. A local example of the expected shape is:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl -n urbanpulse create secret generic urbanpulse-secrets \
  --from-literal=GOLEMIO_API_KEY='replace-me' \
  --from-literal=POSTGRES_PASSWORD='replace-me'
```

Images published by GitHub Actions can be wired into the Kustomize base.

```bash
cd deploy/kubernetes
kustomize edit set image urbanpulse/frontend=ghcr.io/<owner>/<repo>/frontend:latest
kustomize edit set image urbanpulse/realtime-gateway=ghcr.io/<owner>/<repo>/realtime-gateway:latest
kustomize edit set image urbanpulse/data-service=ghcr.io/<owner>/<repo>/data-service:latest
```

For repeatable deployments, a `sha-<commit>` image tag is preferred over
`latest`.

The base is applied as one Kustomize unit.

```bash
kubectl apply -k deploy/kubernetes
kubectl -n urbanpulse get pods
```

Local inspection without ingress has been done through port forwarding.

```bash
kubectl -n urbanpulse port-forward svc/frontend 5173:5173
kubectl -n urbanpulse port-forward svc/realtime-gateway 3000:3000
kubectl -n urbanpulse port-forward svc/data-api 8000:8000
```

For a real ingress, `VITE_WS_URL` in `configmap.yaml` is expected to point at
the public WebSocket address, usually a `wss://.../ws` URL.

## What Comes Next

This Kubernetes layer is not the final production story yet. Resource requests,
persistent volume decisions, ingress, TLS, secret management, observability, and
database backup handling still need a more deliberate design. A later AWS
version is expected to be managed with Terraform once the local deployment shape
has become stable.
