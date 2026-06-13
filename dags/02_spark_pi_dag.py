"""
DAG 02: Spark Pi — Airflow → Spark Operator integration

Submit SparkPi job với resource profile được chọn lúc trigger.
Xem kết quả Pi trong task logs.

Trigger: UI → Trigger DAG w/ config → chọn spark_profile (small/medium/large/xlarge)
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from spark_profiles import SparkJobConfig, make_spark_submit_task, spark_profile_param

SPARK_PI = SparkJobConfig(
    name="spark-pi-airflow",
    main_class="org.apache.spark.examples.SparkPi",
    main_application_file="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
    arguments=["50"],
)

with DAG(
    dag_id="02_spark_pi",
    description="Submit Spark Pi với profile có thể chọn khi trigger",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "integration"],
    params={"spark_profile": spark_profile_param()},
) as dag:

    submit = make_spark_submit_task("submit_spark_pi", SPARK_PI)

    def show_result(**context):
        print("Xem kết quả Pi trong logs của task 'submit_spark_pi'")
        print("Hoặc: kubectl logs -n data-processing spark-pi-airflow-driver")

    result = PythonOperator(task_id="show_result", python_callable=show_result)

    submit >> result
