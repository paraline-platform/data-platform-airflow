# Base: image chính thức từ Helm chart Apache Airflow
# Phải khớp với version trong platform/values/base/airflow.yaml
FROM apache/airflow:2.10.4-python3.11

# Bake DAGs vào image → không cần kubectl cp thủ công
# Lợi ích: immutable image, rollback dễ dàng, mọi pod (scheduler/worker) dùng cùng DAGs
COPY dags/ /opt/airflow/dags/

# Providers cần thiết — dùng constraint file để đảm bảo tương thích với Airflow 2.10.4
RUN pip install --no-cache-dir \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-amazon \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
