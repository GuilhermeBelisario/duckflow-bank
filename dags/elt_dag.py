from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from dateime import datetime
import glob
import shutil


@dag("event_driven_dag", start_date=datetime(2024, 1, 1), schedule=None)
def processador():

    sensor_clientes = FileSensor(
        task_id="sensor_de_arquivos",
        filepath="data/bronze/clientes/*json",
        poke_interval=30,
    )

    sensor_contas = FileSensor(
        task_id="sensor_de_arquivos",
        filepath="data/bronze/contas/*.json",
        poke_interval=30,
    )

    sensor_cartoes = FileSensor(
        task_id="sensor_de_arquivos",
        filepath="data/bronze/cartoes/*.json",
        poke_interval=30,
    )

    sensor_pix = FileSensor(
        task_id="sensor_de_arquivos",
        filepath="data/bronze/transacoes_pix/*.json",
        poke_interval=30,
    )

    sensor_transacoes_cartao = FileSensor(
        task_id="sensor_de_arquivos",
        filepath="data/bronze/transacoes_cartao/*.json",
        poke_interval=30,
    )

    @task
    def processar_silver_padronizados():

        con = duckdb.connect()
        DESTINO = "data/bronze"

        entidades = [
            "clientes",
            "contas",
            "cartoes",
            "transacoes_pix",
            "transacoes_cartao",
        ]
        Path("data/silver").mkdir(parents=True, exist_ok=True)

        for entidade in entidades:
            Path(f"data/silver/{entidade}").mkdir(parents=True, exist_ok=True)

            query = f"""SELECT * FROM read_json_auto('data/landing/{entidade}/*.json' LIMIT 1)"""

            df = con.query(query).fetchdf()

            if df:

                query = f"""
                    COPY (
                        SELECT * FROM read_json_auto('data/landing/{entidade}/*.json')
                    ) TO 'data/silver/{entidade}/{entidade}_silver{(datetime.now()).strftime("%Y-%m-%d-%H-%M-%S")}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
                con.execute(query)

                for arquivo in glob.glob(f"data/landing/{entidade}/*.json"):
                    Path(f"{DESTINO}/{entidade}").mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.move(arquivo, f"{DESTINO}/{entidade}/")
                        print(
                            f"Sucesso ao mover o arquivo: {arquivo} para {DESTINO}/{entidade}/"
                        )
                    except:
                        print(f"Erro ao mover arquivo {arquivo}")
            else:
                print(f"Nenhum arquivo encontrado na entidade {entidade}")

        con.close()
        return "Camada Silver (Parquet) Processada com sucesso."

    @task
    def processar_gold_agregacoes(status_validacao):
        con = duckdb.connect()
        Path("data/gold/resumos").mkdir(parents=True, exist_ok=True)

        # Cria uma tabela resumo (Gold) de transações PIX por tipo de chave
        query_pix_resumo = """
            COPY (
                SELECT 
                    data_processamento,
                    tipo_chave,
                    status,
                    COUNT(id_transacao) as qtd_transacoes,
                    SUM(valor) as valor_total
                FROM read_parquet('data/silver/transacoes_pix/*.parquet')
                GROUP BY 1, 2, 3
            ) TO 'data/gold/resumos/pix_diario.parquet' (FORMAT PARQUET);
        """
        con.execute(query_pix_resumo)

        # Outras agregações (exemplos: média de gastos com cartão de crédito, saldo de contas, etc)

        con.close()
        return "Camada Gold gerada"

    # Definindo as dependências (Ordem de Execução)
    (
        processar_silver_padronizados()
        >> validar_qualidade_silver()
        >> processar_gold_agregacoes()
    )
