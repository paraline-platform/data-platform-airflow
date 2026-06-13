"""
DAG 01: Hello Airflow — Sanity check

Mục đích: verify Airflow + KubernetesExecutor hoạt động đúng
Không cần kết nối external (MinIO, Kafka, Spark).

Concepts minh hoạ:
  - DAG definition với context manager
  - BashOperator: chạy shell command trong task pod
  - PythonOperator: chạy Python function trong task pod
  - Task dependencies: task1 >> task2 >> task3
  - XCom: truyền data giữa tasks qua return value

Cách chạy:
  Airflow UI → DAGs → 01_hello_airflow → Trigger DAG
"""

from __future__ import annotations

import platform
import socket
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ============================================================
# DAG Definition
# ============================================================
# dag_id: tên unique, hiện trong UI
# schedule: None = manual trigger only
# catchup: False = không chạy backfill cho dates trong quá khứ
# tags: nhóm DAGs trong UI
with DAG(
    dag_id="01_hello_airflow",
    description="Sanity check: verify Airflow + KubernetesExecutor",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "hello"],
    doc_md=__doc__,
) as dag:

    # ---- Task 1: BashOperator ----
    # Mỗi task chạy trong 1 pod riêng (KubernetesExecutor)
    # Pod được tạo khi task sẵn sàng, xóa khi hoàn thành
    print_env = BashOperator(
        task_id="print_env",
        bash_command="""
            echo "=== Airflow Task Pod Info ==="
            echo "Hostname (pod name): $(hostname)"
            echo "Date: $(date)"
            echo "Airflow version: $(airflow version)"
            echo "Python: $(python --version)"
            echo ""
            echo "=== Kubernetes Info ==="
            echo "Namespace: $(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)"
            echo "Node: ${MY_NODE_NAME:-unknown}"
        """,
    )

    # ---- Task 2: PythonOperator ----
    # context: dict chứa metadata của task run hiện tại
    # return value tự động push vào XCom với key "return_value"
    def log_context(**context) -> dict:
        """In thông tin Airflow context và return để dùng qua XCom."""
        info = {
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "task_id": context["task_instance"].task_id,
            "execution_date": str(context["logical_date"]),
            "python_version": platform.python_version(),
            "hostname": socket.gethostname(),
        }
        for key, val in info.items():
            print(f"  {key}: {val}")
        return info  # → push vào XCom

    log_run_info = PythonOperator(
        task_id="log_run_info",
        python_callable=log_context,
    )

    # ---- Task 3: Đọc XCom từ task trước ----
    # XCom (Cross-Communication) là cơ chế truyền data nhỏ giữa tasks
    # Dùng cho metadata, file paths — KHÔNG dùng cho data lớn (dùng MinIO thay)
    def read_xcom(**context) -> None:
        ti = context["task_instance"]
        # pull XCom từ task "log_run_info" trong cùng DAG run
        data = ti.xcom_pull(task_ids="log_run_info")
        print(f"=== Data từ task trước (XCom) ===")
        for key, val in (data or {}).items():
            print(f"  {key}: {val}")
        print("\n✓ DAG 01_hello_airflow hoàn thành thành công!")

    verify_xcom = PythonOperator(
        task_id="verify_xcom",
        python_callable=read_xcom,
    )

    # ---- Task Dependencies ----
    # >> là cú pháp Airflow để định nghĩa dependency
    # A >> B = B chạy SAU khi A hoàn thành thành công
    print_env >> log_run_info >> verify_xcom
