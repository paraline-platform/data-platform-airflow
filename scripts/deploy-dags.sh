#!/usr/bin/env bash
# ============================================================
# DEPLOY-DAGS.SH — Copy DAGs từ local vào Airflow scheduler pod
# ============================================================
# Usage:
#   ./scripts/deploy-dags.sh [env]
#
# Arguments:
#   env: dev (default) | uat | prod
#
# Cách hoạt động:
#   1. Tìm scheduler pod trong namespace data-orchestration
#   2. Copy tất cả .py files trong dags/ vào pod's /opt/airflow/dags/
#   3. Airflow scheduler tự detect file mới trong vòng ~30 giây
#
# Tại sao dùng kubectl cp thay vì hostPath?
#   - Không phụ thuộc node hostname (dynamic, reusable cho mọi env)
#   - Ghi vào PVC → tất cả Airflow pods (kể cả task pods) đọc được
#   - Không cần biết pod đang chạy trên node nào
# ============================================================

set -euo pipefail

ENV=${1:-dev}
NAMESPACE="data-orchestration"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DAGS_DIR="$ROOT_DIR/dags"

# --- Validate ---
if [[ ! -d "$DAGS_DIR" ]]; then
  echo "ERROR: dags/ directory not found at $DAGS_DIR"
  exit 1
fi

DAG_COUNT=$(find "$DAGS_DIR" -name "*.py" | wc -l | tr -d ' ')
if [[ "$DAG_COUNT" -eq 0 ]]; then
  echo "WARNING: No .py files found in $DAGS_DIR"
  exit 0
fi

# --- Tìm scheduler pod (dynamic — không hardcode tên pod) ---
echo "==> Finding Airflow scheduler pod in $NAMESPACE..."
SCHEDULER=$(kubectl get pods \
  -n "$NAMESPACE" \
  -l component=scheduler \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -z "$SCHEDULER" ]]; then
  echo "ERROR: Airflow scheduler pod not found or not Running in $NAMESPACE"
  echo "       Deploy Airflow trước: ./scripts/deploy.sh $ENV 06-orchestration"
  exit 1
fi
echo "==> Scheduler pod: $SCHEDULER"

# --- Đảm bảo thư mục DAGs tồn tại trong pod ---
kubectl exec -n "$NAMESPACE" "$SCHEDULER" -- mkdir -p /opt/airflow/dags

# --- Copy DAGs ---
echo ""
echo "==> Syncing $DAG_COUNT DAG(s) to $SCHEDULER:/opt/airflow/dags/"
echo ""

COPIED=0
FAILED=0

while IFS= read -r dag_file; do
  filename=$(basename "$dag_file")
  if kubectl cp "$dag_file" "$NAMESPACE/$SCHEDULER:/opt/airflow/dags/$filename" 2>/dev/null; then
    echo "    ✓ $filename"
    ((COPIED++)) || true
  else
    echo "    ✗ $filename (FAILED)"
    ((FAILED++)) || true
  fi
done < <(find "$DAGS_DIR" -name "*.py" -type f)

echo ""
echo "==> Result: $COPIED copied, $FAILED failed"
echo ""
echo "==> Airflow scheduler sẽ detect DAGs mới trong ~30 giây."
echo "    Xem DAGs trong UI: kubectl port-forward svc/airflow-webserver 8080:8080 -n $NAMESPACE"
echo "    Sau đó mở: http://localhost:8080 (admin / airflow-dev)"
