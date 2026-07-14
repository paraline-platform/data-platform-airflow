"""
DAG 05: dbt — Airflow chạy dbt trên lakehouse (Iceberg qua Spark Thrift)

dbt giờ chạy TRONG Airflow (luồng điều phối DUY NHẤT), thay cho
processing/scripts/run-dbt.sh. Pod dùng image dbt (bake sẵn dbt project +
profiles.yml bởi processing/docker/dbt/Dockerfile), kết nối Spark Thrift Server
ở namespace data-modeling.

Config do infra inject (KHÔNG hardcode trong DAG) — đọc bằng Jinja lúc chạy:
  - var.value.dbt_image  → image dbt (ghcr, theo env)          [AIRFLOW_VAR_DBT_IMAGE]
  - var.json.dbt.schema  → schema đích (dbt_stg/uat/prd)        [AIRFLOW_VAR_DBT]
  - var.json.dbt.threads → số thread                            [AIRFLOW_VAR_DBT]
  - var.value.env        → tên env = dbt --target              [AIRFLOW_VAR_ENV]

profiles.yml (trong image) đọc DBT_TARGET/DBT_SCHEMA/DBT_THREADS từ env vars;
DBT_HOST/DBT_PORT đã có default trỏ spark-thrift-server.data-modeling:10000.

Trigger: UI → Trigger DAG w/ config → chọn dbt_command (run/test/build/…) + select (tùy chọn).
Ví dụ: {"dbt_command": "run", "select": "staging"}
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from lib.dbt import dbt_command_param, dbt_select_param

# Namespace chứa Spark Thrift Server + nơi dbt Job chạy (khớp run-dbt.sh cũ).
NAMESPACE = "data-modeling"

with DAG(
    dag_id="05_dbt",
    description="Chạy dbt (run/test/build…) trên lakehouse qua Spark Thrift",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "modeling", "lakehouse"],
    doc_md=__doc__,
    params={
        "dbt_command": dbt_command_param(),
        "select": dbt_select_param(),
    },
) as dag:

    # KubernetesPodOperator: chạy image dbt như một pod thường (dbt chỉ cần kết nối
    # Thrift qua network → không cần quyền K8s API → SA mặc định của namespace là đủ).
    #
    # Image ENTRYPOINT = ["dbt"], nhưng ta override cmds=["sh","-c"] để ghép câu lệnh
    # có --select TÙY CHỌN gọn trong 1 chuỗi (select rỗng → không thêm cờ).
    run_dbt = KubernetesPodOperator(
        task_id="dbt",
        name="dbt-{{ params.dbt_command }}",
        namespace=NAMESPACE,
        image="{{ var.value.dbt_image }}",
        image_pull_policy="IfNotPresent",
        cmds=["sh", "-c"],
        arguments=[
            "dbt {{ params.dbt_command }} --target {{ var.value.env }}"
            "{{ (' --select ' ~ params.select) if params.select else '' }}"
        ],
        env_vars={
            "DBT_TARGET": "{{ var.value.env }}",
            "DBT_SCHEMA": "{{ var.json.dbt.schema }}",
            "DBT_THREADS": "{{ var.json.dbt.threads }}",
        },
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,             # stream log dbt vào task log của Airflow
        on_finish_action="delete_succeeded_pod",  # giữ pod fail để debug, xóa pod thành công
        do_xcom_push=False,
    )
