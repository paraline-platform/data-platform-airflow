# data-platform-airflow

Airflow DAGs and orchestration config for the Data Lakehouse Platform. Orchestrates Spark jobs, dbt runs, and data pipeline workflows.

## Structure

```
dags/
  01_hello_airflow.py      # Sanity-check DAG
  02_spark_pi_dag.py       # Spark Pi via SparkKubernetesOperator
  03_minio_dag.py          # MinIO read/write test
  04_data_pipeline_dag.py  # Full pipeline: ingest → Spark → dbt → verify
  spark_profiles.py        # Spark resource profile helpers (reads Airflow Variable)
docker/
  Dockerfile               # Custom Airflow image with DAGs baked in (Phase 3 CI)
scripts/
  deploy-dags.sh           # Dev helper: kubectl cp DAGs into scheduler pod
```

## Deploy DAGs (dev — manual)

```bash
# Copy DAGs into running Airflow scheduler pod
./scripts/deploy-dags.sh dev

# Access UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-orchestration
# open http://localhost:8080  (admin / airflow-dev)
```

## Airflow Variables required at runtime

| Variable | Description | Example |
|---|---|---|
| `spark_profiles` | JSON of Spark resource profiles | `{"small": {...}, "medium": {...}}` |

Set via UI: Admin → Variables, or:
```bash
kubectl exec -n data-orchestration <scheduler-pod> -- \
  airflow variables set spark_profiles '{"small": {...}}'
```

## DAG Overview

| DAG | Description |
|---|---|
| `hello_airflow` | Sanity check, no external deps |
| `spark_pi_dag` | SparkKubernetesOperator → Spark Operator |
| `minio_dag` | S3Hook read/write to MinIO |
| `data_pipeline` | Full ETL: S3 ingest → Spark → dbt → verify |
