# Base: image chính thức từ Helm chart Apache Airflow
# Phải khớp với version trong platform/values/base/airflow.yaml
FROM apache/airflow:2.10.4-python3.11

# Bake DAGs vào image → không cần kubectl cp thủ công
# Lợi ích: immutable image, rollback dễ dàng, mọi pod (scheduler/worker) dùng cùng DAGs
COPY dags/ /opt/airflow/dags/

# Providers cần thiết cho DAGs trong repo này
RUN pip install --no-cache-dir \
    apache-airflow-providers-cncf-kubernetes==8.4.2 \
    apache-airflow-providers-amazon==8.30.0
