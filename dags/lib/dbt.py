"""
dbt helpers cho Airflow DAG (chuyển dbt vào Airflow — luồng điều phối duy nhất).

Config dbt do infra inject qua env var lúc pod Airflow start (xem
infra: platform/values/env/airflow.yaml.gotmpl):
  - AIRFLOW_VAR_DBT       → Variable "dbt"       = {"schema": "...", "threads": N}
  - AIRFLOW_VAR_DBT_IMAGE → Variable "dbt_image" = image dbt (ghcr, theo env)
  - AIRFLOW_VAR_ENV       → Variable "env"       = tên env (=dbt --target)

DAG đọc bằng Jinja lúc CHẠY (var.value.* / var.json.*) — KHÔNG gọi Variable.get ở
top-level module (tránh hit DB mỗi nhịp scheduler; giống cách specs dùng var.json).
"""
from __future__ import annotations

# Lệnh dbt hợp lệ cho dropdown khi trigger DAG. Chỉ là danh sách tên — giá trị
# thật (schema/threads/image) nằm ở Airflow Variable do infra inject.
DBT_COMMANDS = ["run", "build", "test", "seed", "snapshot", "compile"]


def dbt_command_param(default: str = "run"):
    """Airflow Param cho dropdown chọn lệnh dbt lúc trigger DAG."""
    from airflow.models.param import Param

    return Param(
        default,
        enum=DBT_COMMANDS,
        description="Lệnh dbt chạy khi trigger (run/build/test/seed/snapshot/compile)",
    )


def dbt_select_param(default: str = ""):
    """Airflow Param (tùy chọn) để giới hạn model: dbt --select <value>."""
    from airflow.models.param import Param

    return Param(
        default,
        type="string",
        description="(Tùy chọn) dbt --select, ví dụ: staging hoặc marts.products_by_category",
    )
