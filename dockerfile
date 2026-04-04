FROM apache/airflow:3.0.0

COPY prod_requirements.txt /opt/airflow/prod_requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/prod_requirements.txt