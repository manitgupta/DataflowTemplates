#!/usr/bin/env bash
# GKE cluster arango-flink (arango-vpc) running the Flink Kubernetes Operator, sized like the Spanner Migration Omni
# POC (~/spanner-migration-omni/beam/infra/04-gke.sh): flink-workers = n2-standard-16 (one TaskManager per node),
# jm-pool = n2-standard-8 (JobManager). Private nodes (Cloud NAT arango-router), DNS endpoint for kubectl.
# Usage: 06-gke-flink.sh [workers=32]   (idempotent; scale later with gcloud container clusters resize)
set -euo pipefail
source "$(dirname "$0")/env.sh"
W=${1:-32}
SA=omni-runner@${PROJECT}.iam.gserviceaccount.com
HERE=$(cd "$(dirname "$0")/.." && pwd)
g() { gcloud --project "$PROJECT" "$@"; }
g container clusters describe arango-flink --zone $ZONE >/dev/null 2>&1 || \
  g container clusters create arango-flink --zone $ZONE --network $NET --subnetwork $SUBNET \
    --enable-ip-alias --cluster-ipv4-cidr=10.40.0.0/14 --services-ipv4-cidr=10.44.0.0/20 \
    --enable-private-nodes --master-ipv4-cidr=172.16.1.0/28 --enable-dns-access \
    --machine-type=e2-standard-4 --num-nodes=1 --service-account=$SA --scopes=cloud-platform \
    --release-channel=regular --labels=env=arango
g container node-pools describe flink-workers --cluster arango-flink --zone $ZONE >/dev/null 2>&1 || \
  g container node-pools create flink-workers --cluster=arango-flink --zone=$ZONE \
    --machine-type=n2-standard-16 --num-nodes=$W --service-account=$SA --scopes=cloud-platform \
    --node-labels=pool=flink-workers --disk-type=pd-ssd --disk-size=200GB
g container node-pools describe jm-pool --cluster arango-flink --zone $ZONE >/dev/null 2>&1 || \
  g container node-pools create jm-pool --cluster=arango-flink --zone=$ZONE \
    --machine-type=n2-standard-8 --num-nodes=1 --service-account=$SA --scopes=cloud-platform
# private kubeconfig: never switch the shared ~/.kube/config context (other sessions use it)
export KUBECONFIG=$HOME/.kube/arango-flink.config
g container clusters get-credentials arango-flink --zone $ZONE --dns-endpoint
export PATH=~/bin:$PATH
kubectl create namespace flink --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$HERE/flink/flink/k8s/rbac.yaml"
# Operator 1.16.1 without the admission webhook (no cert-manager needed).
helm repo add flink-operator-1.16.1 https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/ >/dev/null 2>&1 || true
helm status flink-kubernetes-operator -n flink-operator >/dev/null 2>&1 || \
  helm install flink-kubernetes-operator flink-operator-1.16.1/flink-kubernetes-operator \
    --namespace flink-operator --create-namespace --set webhook.create=false --wait --timeout 10m
kubectl get nodes -L pool --no-headers | awk '{print $NF}' | sort | uniq -c
kubectl -n flink-operator get pods
