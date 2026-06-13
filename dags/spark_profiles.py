"""
Spark resource profiles và task factory cho Airflow DAGs.

Profiles được định nghĩa trong platform/spark-profiles/<env>.yaml
và inject vào Airflow qua AIRFLOW_VAR_SPARK_PROFILES khi deploy.

─────────────────────────────────────────────────────────────
Cách dùng trong DAG — 3 bước:

    from spark_profiles import SparkJobConfig, spark_profile_param, make_spark_submit_task

    # 1. Khai báo job — chỉ define WHAT (không hardcode resources)
    SPARK_PI = SparkJobConfig(
        name="spark-pi-airflow",
        main_class="org.apache.spark.examples.SparkPi",
        main_application_file="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
        arguments=["50"],
    )

    # 2. DAG params — hiển thị dropdown trong UI với thông tin resource
    with DAG(..., params={"spark_profile": spark_profile_param()}):

        # 3. Tạo task — build spec + submit gộp làm 1
        submit = make_spark_submit_task("submit_spark_pi", SPARK_PI)
        submit >> show_result

Trigger từ UI: Trigger DAG w/ config → chọn spark_profile
─────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import copy
import json
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

# Profiles cứng — dùng làm fallback và để build Param description lúc DAG parse
# (Variable.get() chỉ gọi được lúc task chạy, không lúc DAG parse)
_FALLBACK_PROFILES = {
    "small": {
        "driver":   {"cores": 1, "coreLimit": "1",  "memory": "512m"},
        "executor": {"instances": 1, "cores": 1, "coreLimit": "1",  "memory": "512m"},
    },
    "medium": {
        "driver":   {"cores": 2, "coreLimit": "2",  "memory": "1g"},
        "executor": {"instances": 2, "cores": 2, "coreLimit": "2",  "memory": "2g"},
    },
    "large": {
        "driver":   {"cores": 2, "coreLimit": "2",  "memory": "2g"},
        "executor": {"instances": 2, "cores": 2, "coreLimit": "2",  "memory": "4g"},
    },
    "xlarge": {
        "driver":   {"cores": 4, "coreLimit": "4",  "memory": "4g"},
        "executor": {"instances": 2, "cores": 4, "coreLimit": "4",  "memory": "8g"},
    },
}

VALID_PROFILES = list(_FALLBACK_PROFILES.keys())


# ============================================================
# Profile helpers
# ============================================================

def _get_profiles() -> dict:
    """Đọc profiles từ Airflow Variable (runtime only)."""
    try:
        from airflow.models import Variable
        raw = Variable.get("spark_profiles", default_var="{}")
        profiles = json.loads(raw)
        if profiles:
            return profiles
    except Exception as e:
        log.debug("Cannot read spark_profiles Variable: %s — using fallback", e)
    return _FALLBACK_PROFILES


def spark_profile_param(default: str = "small"):
    """Return Airflow Param cho spark_profile với description hiển thị resource table.

    Dùng trong DAG params:
        params={"spark_profile": spark_profile_param()}

    UI sẽ hiện dropdown với description:
        small  — driver: 1c / 512m  │ executor: 1 × 1c / 512m
        medium — driver: 2c / 1g    │ executor: 2 × 2c / 2g
        ...
    """
    from airflow.models.param import Param

    lines = ["Resource profiles (từ spark-profiles/<env>.yaml):\n"]
    for name, p in _FALLBACK_PROFILES.items():
        d, e = p["driver"], p["executor"]
        lines.append(
            f"  {name:<8} — driver: {d['cores']}c / {d['memory']:<6}"
            f"  │  executor: {e['instances']} × {e['cores']}c / {e['memory']}"
        )
    description = "\n".join(lines)

    return Param(
        default,
        enum=VALID_PROFILES,
        description=description,
    )


def build_spark_spec(base_spec: dict, profile: str = "small") -> dict:
    """Merge SparkApplication base spec với driver/executor resources từ profile.

    Args:
        base_spec: Dict SparkApplication — define WHAT (image, mainClass, args...)
                   Không cần điền resources — sẽ được fill từ profile.
        profile:   "small" | "medium" | "large" | "xlarge"
    """
    profiles = _get_profiles()
    p = profiles.get(profile)
    if not p:
        log.warning("Profile '%s' not found, falling back to 'small'", profile)
        p = profiles.get("small", _FALLBACK_PROFILES["small"])

    spec = copy.deepcopy(base_spec)
    spec.setdefault("spec", {}).setdefault("driver", {}).update({
        "cores":     p["driver"]["cores"],
        "coreLimit": p["driver"]["coreLimit"],
        "memory":    p["driver"]["memory"],
    })
    spec["spec"].setdefault("executor", {}).update({
        "instances": p["executor"]["instances"],
        "cores":     p["executor"]["cores"],
        "coreLimit": p["executor"]["coreLimit"],
        "memory":    p["executor"]["memory"],
    })
    return spec


# ============================================================
# Spark job config & task factory
# ============================================================

@dataclass
class SparkJobConfig:
    """Khai báo 1 Spark job — chỉ define WHAT, không define resource HOW.

    Resources được inject khi task chạy từ profile được chọn lúc trigger.

    Attributes:
        name:                   Tên SparkApplication trong K8s (metadata.name)
        main_class:             Fully-qualified class name (Scala) hoặc Python script path
        main_application_file:  JAR path hoặc Python file path
        image:                  Docker image chứa Spark runtime
        arguments:              Args truyền vào main class/script
        spark_version:          Phải match với image
        namespace:              Namespace để submit SparkApplication
        service_account:        SA có quyền tạo executor pods
        extra_driver:           Fields bổ sung cho spec.driver (labels, env, volumeMounts...)
        extra_executor:         Fields bổ sung cho spec.executor
        spark_conf:             sparkConf dict (e.g. S3A settings)
    """
    name: str
    main_class: str
    main_application_file: str
    image: str = "apache/spark:3.5.3"
    arguments: list = field(default_factory=list)
    spark_version: str = "3.5.3"
    namespace: str = "data-processing"
    service_account: str = "spark-operator-spark"
    extra_driver: dict = field(default_factory=dict)
    extra_executor: dict = field(default_factory=dict)
    spark_conf: dict = field(default_factory=dict)

    def base_spec(self) -> dict:
        """Build base SparkApplication spec (không có resources)."""
        spec: dict = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": self.name, "namespace": self.namespace},
            "spec": {
                "type": "Scala",
                "mode": "cluster",
                "image": self.image,
                "imagePullPolicy": "IfNotPresent",
                "mainClass": self.main_class,
                "mainApplicationFile": self.main_application_file,
                "sparkVersion": self.spark_version,
                "restartPolicy": {"type": "Never"},
                "driver": {
                    "serviceAccount": self.service_account,
                    "labels": {"trigger": "airflow"},
                    **self.extra_driver,
                },
                "executor": {
                    "labels": {"trigger": "airflow"},
                    **self.extra_executor,
                },
            },
        }
        if self.arguments:
            spec["spec"]["arguments"] = self.arguments
        if self.spark_conf:
            spec["spec"]["sparkConf"] = self.spark_conf
        return spec


def make_spark_submit_task(task_id: str, config: SparkJobConfig):
    """Tạo 1 PythonOperator gộp build-spec + submit SparkApplication.

    Tại runtime: đọc params.spark_profile → build spec từ profile → submit.
    Không cần split thành 2 task (build_spec + submit) và không cần XCom workaround.

    Args:
        task_id: task_id trong DAG (ví dụ: "submit_spark_pi")
        config:  SparkJobConfig định nghĩa job

    Returns:
        PythonOperator — có thể wire vào dependencies như bình thường.

    Usage:
        submit = make_spark_submit_task("submit_spark_pi", SPARK_PI)
        submit >> next_task
    """
    import yaml
    from airflow.operators.python import PythonOperator
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
        SparkKubernetesOperator,
    )

    def _run(**context):
        profile = context["params"].get("spark_profile", "small")
        spec = build_spark_spec(config.base_spec(), profile)
        log.info(
            "Submitting SparkApplication '%s' with profile '%s' "
            "(driver: %sc/%s, executor: %s×%sc/%s)",
            config.name, profile,
            spec["spec"]["driver"]["cores"], spec["spec"]["driver"]["memory"],
            spec["spec"]["executor"]["instances"],
            spec["spec"]["executor"]["cores"], spec["spec"]["executor"]["memory"],
        )
        op = SparkKubernetesOperator(
            task_id=config.name,   # valid K8s name → SparkApplication name: {config.name}-{random}
            namespace=config.namespace,
            application_file=yaml.dump(spec),
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
        return op.execute(context)

    return PythonOperator(task_id=task_id, python_callable=_run)
