from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from pathlib import Path
from utils.constants import entidades
import glob
import duckdb
import shutil
from time import sleep


@dag("event_driven_dag", start_date=datetime.now(), schedule="*/1 * * * *")
def processador():

    sensor_clientes = FileSensor(
        task_id="sensor_de_arquivos_clientes",
        filepath="data/landing/clientes/*.json",
        fs_conn_id='fs_default',
        poke_interval=30,
    )

    sensor_contas = FileSensor(
        task_id="sensor_de_arquivos_contas",
        filepath="data/landing/contas/*.json",
        fs_conn_id='fs_default',
        poke_interval=30,
    )

    sensor_cartoes = FileSensor(
        task_id="sensor_de_arquivos_cartoes",
        filepath="data/landing/cartoes/*.json",
        fs_conn_id='fs_default',
        poke_interval=30,
    )

    sensor_pix = FileSensor(
        task_id="sensor_de_arquivos_pix",
        filepath="data/landing/transacoes_pix/*.json",
        fs_conn_id='fs_default',
        poke_interval=30,
    )

    sensor_transacoes_cartao = FileSensor(
        task_id="sensor_de_arquivos_transacoes_cartao",
        filepath="data/landing/transacoes_cartao/*.json",
        fs_conn_id='fs_default',
        poke_interval=30,
    )

    @task
    def processar_bronze_padronizados():

        sleep(5)

        con = duckdb.connect()
        DESTINO = "data/bronze"
        DESTINO_PROCESSADOS = "data/landing_processados"

        for entidade in entidades:
            Path(f"data/bronze/{entidade}").mkdir(parents=True, exist_ok=True)

            query = f"""SELECT * FROM read_json_auto('data/landing/{entidade}/*.json') LIMIT 1"""

            df = con.query(query).fetchone()

            if df:

                query = f"""
                    COPY (
                        SELECT * FROM read_json_auto('data/landing/{entidade}/*.json')
                    ) TO 'data/bronze/{entidade}/{entidade}_bronze{(datetime.now()).strftime("%Y-%m-%d-%H-%M-%S")}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
                con.execute(query)

                Path(f"{DESTINO_PROCESSADOS}").mkdir(parents=True, exist_ok=True)

                for arquivo in glob.glob(f"data/landing/{entidade}/*.json"):

                    Path(f"{DESTINO_PROCESSADOS}/{entidade}").mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.move(arquivo, f"{DESTINO_PROCESSADOS}/{entidade}/")
                        print(
                            f"Sucesso ao mover o arquivo: {arquivo} para {DESTINO_PROCESSADOS}/{entidade}/"
                        )
                    except:
                        print(f"Erro ao mover arquivo {arquivo}")
            else:
                print(f"Nenhum arquivo encontrado na entidade {entidade}")

        con.close()
        return "Camada bronze (Parquet) Processada com sucesso."

    @task
    def processar_silver_agregacoes():

        con = duckdb.connect()

        for entidade in entidades:

            Path(f"data/silver/{entidade}").mkdir(parents=True, exist_ok=True)

            con.close()
            return print("Arquivos padronizados!")

    sensores = [
        sensor_clientes,
        sensor_contas,
        sensor_cartoes,
        sensor_pix,
        sensor_transacoes_cartao,
    ]

    (
        sensores >> processar_bronze_padronizados() >> processar_silver_agregacoes()
    )
       
dag_pipeline = processador()
