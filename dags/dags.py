import subprocess
from pathlib import Path
from airflow.sdk import dag, task
import duckdb

@dag(
    dag_id="gerador_de_dados", 
    schedule="*/1 * * * *", 
    catchup=False
)
def bank_data_processing():

    @task
    def gerar_dados_bronze():
        subprocess.run(["python", "data/generate_data.py"], check=True)
        return "Dados gerados na camada bronze"

    gerar_dados_bronze()

dag_pipeline = bank_data_processing()
