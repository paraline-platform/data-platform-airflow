"""
DAG 02: Spark Pi — Airflow → Spark Operator integration

Submit SparkPi job với resource profile được chọn lúc trigger.
Xem kết quả Pi trong task logs.

Trigger: UI → Trigger DAG w/ config → chọn spark_profile (small/medium/large/xlarge)

P2.3: dùng SparkKubernetesOperator TRỰC TIẾP (không bọc trong PythonOperator)
→ on_kill hoạt động (kill task = kill Spark job), retry/deferrable/UI chuẩn.
Spec là Jinja template: dags/specs/spark-pi.yaml (profile render lúc chạy).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

from lib.spark_profiles import spark_profile_param

with DAG(
    dag_id="02_spark_pi",
    description="Submit Spark Pi với profile có thể chọn khi trigger",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "integration"],
    params={"spark_profile": spark_profile_param()},
) as dag:

    submit = SparkKubernetesOperator(
        task_id="submit_spark_pi",
        namespace="data-processing",
        application_file="specs/spark-pi.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    def show_result(**context):
        print("Xem kết quả Pi trong logs của task 'submit_spark_pi'")
        print("Hoặc: kubectl logs -n data-processing -l trigger=airflow --tail=50")

    result = PythonOperator(task_id="show_result", python_callable=show_result)

    submit >> result
