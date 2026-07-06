"""
Spark resource profile helpers cho Airflow DAGs (P2.3/P2.4).

Profiles định nghĩa trong platform/spark-profiles/<env>.yaml (infra repo),
inject vào Airflow qua AIRFLOW_VAR_SPARK_PROFILES khi deploy.

Cách dùng trong DAG:

    from lib.spark_profiles import spark_profile_param
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
        SparkKubernetesOperator,
    )

    with DAG(..., params={"spark_profile": spark_profile_param()}):
        submit = SparkKubernetesOperator(
            task_id="submit_spark",
            namespace="data-processing",
            application_file="specs/spark-pi.yaml",   # Jinja template — xem dags/specs/
            kubernetes_conn_id="kubernetes_default",
        )

Vì sao KHÔNG còn make_spark_submit_task (bọc operator trong PythonOperator)?
  - on_kill không hoạt động → kill task trên UI không kill Spark job (rò rỉ
    tài nguyên thật trên cluster)
  - Mất retry semantics per-operator, mất deferrable mode, UI sai loại task
  - Spec giờ là Jinja template (dags/specs/*.yaml) do operator THẬT render:
    profile đọc từ {{ params.spark_profile }} + var.json.spark_profiles
Chi tiết: docs/optimization/03-p2-code-optimization.md (infra repo), mục P2.3.
"""
from __future__ import annotations

# Tên profile hợp lệ — phải khớp keys trong spark-profiles/<env>.yaml.
# CHỈ là danh sách tên (không kèm số liệu) — số liệu thật nằm trong Airflow
# Variable `spark_profiles`; hardcode số ở đây sẽ tạo nguồn sự thật thứ hai
# và UI có thể nói dối khi hai nguồn lệch nhau.
VALID_PROFILES = ["small", "medium", "large", "xlarge"]


def spark_profile_param(default: str = "small"):
    """Airflow Param cho dropdown chọn spark_profile lúc trigger DAG."""
    from airflow.models.param import Param

    return Param(
        default,
        enum=VALID_PROFILES,
        description=(
            "Resource profile cho Spark job. "
            "Giá trị thật (cores/memory) xem: Admin → Variables → spark_profiles"
        ),
    )
