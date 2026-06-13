"""
DAG 03: MinIO Operations — Airflow → MinIO integration

Mục đích: đọc/ghi MinIO từ Airflow task pod.
Dùng S3Hook (aws provider) với endpoint_url trỏ về MinIO thay vì AWS S3.

Concepts minh hoạ:
  - S3Hook: abstraction layer trên boto3, đọc credentials từ Connection
  - Connection "minio_s3": được inject qua AIRFLOW_CONN_MINIO_S3 env var
  - Branching: chọn path thực thi dựa theo điều kiện runtime
  - TaskGroup: nhóm tasks liên quan trong UI

Tại sao dùng S3Hook thay vì boto3 trực tiếp?
  - Connection được quản lý bởi Airflow (không hardcode credentials trong DAG)
  - Dễ swap MinIO dev ↔ AWS S3 prod bằng cách đổi Connection config
  - Code DAG không thay đổi khi đổi environment

Cách chạy:
  Airflow UI → DAGs → 03_minio_operations → Trigger DAG
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# S3Hook nằm trong apache-airflow-providers-amazon
# được cài qua _pip_additional_requirements trong chart
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Connection ID — phải khớp với AIRFLOW_CONN_{ID} trong env var
MINIO_CONN_ID = "minio_s3"
TEST_BUCKET = "landing"
TEST_KEY = "airflow-test/hello.json"


# ============================================================
# Task functions
# ============================================================

def check_minio_connection(**context) -> list[str]:
    """Kết nối MinIO và list tất cả buckets."""
    # S3Hook đọc credentials + endpoint_url từ connection "minio_s3"
    # Không cần truyền access_key/secret_key trong code
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3_client = hook.get_conn()

    response = s3_client.list_buckets()
    bucket_names = [b["Name"] for b in response.get("Buckets", [])]

    print(f"=== MinIO Buckets ({len(bucket_names)}) ===")
    for name in bucket_names:
        print(f"  s3a://{name}/")

    if TEST_BUCKET not in bucket_names:
        raise ValueError(f"Bucket '{TEST_BUCKET}' không tồn tại! Chạy MinIO setup trước.")

    print("\n✓ MinIO kết nối thành công")
    return bucket_names   # push vào XCom


def upload_data(**context) -> str:
    """Tạo JSON payload và upload lên MinIO."""
    # Tạo test payload với metadata của DAG run hiện tại
    payload = {
        "source": "airflow",
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "execution_date": str(context["logical_date"]),
        "message": "Hello từ Airflow DAG!",
        "data": [
            {"product": "product_A", "sales": 100, "date": "2024-01-01"},
            {"product": "product_B", "sales": 250, "date": "2024-01-01"},
            {"product": "product_C", "sales": 175, "date": "2024-01-01"},
        ],
    }
    json_str = json.dumps(payload, ensure_ascii=False, indent=2)

    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    # load_string: upload string as file to S3/MinIO
    hook.load_string(
        string_data=json_str,
        key=TEST_KEY,
        bucket_name=TEST_BUCKET,
        replace=True,   # ghi đè nếu đã tồn tại
    )

    s3_path = f"s3a://{TEST_BUCKET}/{TEST_KEY}"
    print(f"✓ Uploaded: {s3_path}")
    print(f"  Size: {len(json_str)} bytes")
    return s3_path    # push vào XCom


def download_and_verify(**context) -> None:
    """Download từ MinIO và verify nội dung."""
    ti = context["task_instance"]

    # Lấy s3_path từ XCom của task upload
    s3_path = ti.xcom_pull(task_ids="write_ops.upload_data")
    print(f"=== Verifying: {s3_path} ===")

    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    # Kiểm tra file tồn tại
    exists = hook.check_for_key(key=TEST_KEY, bucket_name=TEST_BUCKET)
    assert exists, f"File không tồn tại: {TEST_KEY}"

    # Download và parse JSON
    s3_obj = hook.get_key(key=TEST_KEY, bucket_name=TEST_BUCKET)
    content = s3_obj.get()["Body"].read().decode("utf-8")
    data = json.loads(content)

    print("✓ File tồn tại và đọc được")
    print(f"  dag_id: {data.get('dag_id')}")
    print(f"  Records: {len(data.get('data', []))}")
    print("\n✓ MinIO read/write hoạt động đúng")


def list_objects(**context) -> None:
    """List tất cả objects trong bucket."""
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3_client = hook.get_conn()

    # List tất cả objects trong bucket landing/
    response = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    objects = response.get("Contents", [])

    print(f"=== Objects trong s3a://{TEST_BUCKET}/ ===")
    for obj in objects:
        size_kb = obj["Size"] / 1024
        print(f"  {obj['Key']}  ({size_kb:.1f} KB)")
    print(f"\nTotal: {len(objects)} objects")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="03_minio_operations",
    description="Airflow → MinIO: list buckets, upload, download, verify",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["minio", "storage", "integration"],
    doc_md=__doc__,
) as dag:

    # Task đơn lẻ
    check_conn = PythonOperator(
        task_id="check_minio_connection",
        python_callable=check_minio_connection,
    )

    # TaskGroup: nhóm tasks liên quan trong UI
    # Hiện trong UI như: write_ops.upload_data, write_ops.list_objects
    with TaskGroup(group_id="write_ops") as write_group:
        upload = PythonOperator(
            task_id="upload_data",
            python_callable=upload_data,
        )
        list_obj = PythonOperator(
            task_id="list_objects",
            python_callable=list_objects,
        )
        # upload chạy trước, list sau để thấy file mới
        upload >> list_obj

    verify = PythonOperator(
        task_id="download_and_verify",
        python_callable=download_and_verify,
    )

    # Pipeline: check → (upload → list) → verify
    check_conn >> write_group >> verify
