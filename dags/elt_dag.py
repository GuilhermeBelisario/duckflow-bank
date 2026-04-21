from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
        fs_conn_id="fs_default",
        poke_interval=30,
    )

    sensor_contas = FileSensor(
        task_id="sensor_de_arquivos_contas",
        filepath="data/landing/contas/*.json",
        fs_conn_id="fs_default",
        poke_interval=30,
    )

    sensor_cartoes = FileSensor(
        task_id="sensor_de_arquivos_cartoes",
        filepath="data/landing/cartoes/*.json",
        fs_conn_id="fs_default",
        poke_interval=30,
    )

    sensor_pix = FileSensor(
        task_id="sensor_de_arquivos_pix",
        filepath="data/landing/transacoes_pix/*.json",
        fs_conn_id="fs_default",
        poke_interval=30,
    )

    sensor_transacoes_cartao = FileSensor(
        task_id="sensor_de_arquivos_transacoes_cartao",
        filepath="data/landing/transacoes_cartao/*.json",
        fs_conn_id="fs_default",
        poke_interval=30,
    )

    trigger_ddl = TriggerDagRunOperator(
        task_id="trigger_ddl_dag",
        trigger_dag_id="ddl_dag",
        wait_for_completion=True,
        reset_dag_run=True,
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
                        SELECT *,
                        current_localtimestamp() as timestamp_ingestao_bronze,
                        '{entidade}_bank_crm' as origem_arquivo
                        FROM read_json_auto('data/landing/{entidade}/*.json', filename = true)
                    ) TO 'data/bronze/{entidade}/{entidade}_bronze{(datetime.now()).strftime("%Y-%m-%d-%H-%M-%S")}.parquet'
                    (FORMAT PARQUET, COMPRESSION ZSTD);
                """
                con.execute(query)

                Path(f"{DESTINO_PROCESSADOS}").mkdir(parents=True, exist_ok=True)

                for arquivo in glob.glob(f"data/landing/{entidade}/*.json"):

                    Path(f"{DESTINO_PROCESSADOS}/{entidade}").mkdir(
                        parents=True, exist_ok=True
                    )
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
    def validacao_arquivos_bronze():

        con = duckdb.connect()

        validados = []

        # Criação das Pastas
        for entidade in entidades:

            Path(f"data/silver/{entidade}").mkdir(parents=True, exist_ok=True)

            query = f"""SELECT * FROM read_parquet('data/bronze/{entidade}/*.parquet') LIMIT 1"""

            df = con.query(query).fetchone()

            if df:
                validados.append(True)
            else:
                validados.append(False)

        if all(validados):
            return True
        else:
            return False

    @task.skip_if(validacao_arquivos_bronze() == False)
    def processar_silver():

        con = duckdb.connect("data/silver/duckbank.duckdb")

        con.execute(
            f"""
        MERGE INTO cartoes_cred_debt
            USING(
                SELECT * FROM read_parquet('data/bronze/cartoes/*.parquet')    
            ) as bronze_table
        ON (bronze_table.id_cartao = cartoes__cred_debt.id_cartao)
        WHEN MATCHED THEN UPDATE
        WHEN NOT MATCHED THEN INSERT
        """
        )

        con.execute(
            f"""
        MERGE INTO clientes_cadastrados
            USING(
                SELECT * FROM read_parquet('data/bronze/clientes/*.parquet')    
            ) as bronze_table
        ON (bronze_table.id_cliente = clientes_cadastrados.id_cliente)
        WHEN MATCHED THEN UPDATE
        WHEN NOT MATCHED THEN INSERT
        """
        )

        con.execute(
            f"""
        MERGE INTO contas_clientes
            USING(
                SELECT * FROM read_parquet('data/bronze/contas/*.parquet')    
            ) as bronze_table
        ON (bronze_table.id_conta = contas_clientes.id_conta)
        WHEN MATCHED THEN UPDATE
        WHEN NOT MATCHED THEN INSERT
        """
        )

        con.execute(
            f"""
        MERGE INTO transacoes_cred_debt_pix
            USING(
                WITH transacoes_pix (
                    SELECT 
                        id_transacao ,
                        id_conta_origem,
                        id_conta_destino,
                        valor AS valor_total_transacao,
                        chave_pix_destino,
                        tipo_chave AS tipo_chave_pix_destino,
                        descricao AS descricao_pix,
                        status,
                        timestamp AS timestamp_transacao,
                        data_processamento
                    FROM read_parquet('data/bronze/transacoes_pix/*.parquet')
                )
                SELECT 
                    id_transacao,
                    id_cartao,
                    valor_total AS valor_total_transacao,
                    parcelas,
                    valor_parcela,
                    estabelecimento,
                    categoria,
                    cidade_estabelecimento,
                    estado_estabelecimento,
                    modalidade,
                    status,
                    timestamp AS timestamp_transacao,
                    data_processamento
                FROM read_parquet('data/bronze/transacoes_cartao/*.parquet')
                UNION ALL transacoes_pix
            ) as bronze_table
        ON (bronze_table.id_conta = contas_clientes.id_conta)
        WHEN NOT MATCHED THEN INSERT
        """
        )

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
        sensores
        >> processar_bronze_padronizados()
        >> trigger_ddl
        >> validacao_arquivos_bronze()
        >> processar_silver()
    )


dag_pipeline = processador()
