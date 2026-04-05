from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor


@dag("event_driven_dag", start_date=datetime(2024, 1, 1), schedule=None):
def processador():
   
    sensor_clientes = FileSensor(
        task_id="sensor_de_arquivos",
        filepath="data/bronze/clientes/",
        poke_interval=30,
    )

   @task
    def processar_silver_padronizados(status_bronze):
        # Conecta no DuckDB em memória
        con = duckdb.connect()
        
        entidades = ["clientes", "contas", "cartoes", "transacoes_pix", "transacoes_cartao"]
        Path("data/silver").mkdir(parents=True, exist_ok=True)
        
        for entidade in entidades:
            Path(f"data/silver/{entidade}").mkdir(parents=True, exist_ok=True)
            
            query = f"""
                COPY (
                    SELECT * FROM read_json_auto('data/bronze/{entidade}/*.json')
                ) TO 'data/silver/{entidade}/{entidade}_silver.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
            con.execute(query)
            
        con.close()
        return "Camada Silver (Parquet) Processada com sucesso."

    @task
    def validar_qualidade_silver(status_silver):
        con = duckdb.connect()
        
        qtd_clientes = con.execute("SELECT count(*) FROM 'data/silver/clientes/*.parquet'").fetchone()[0]
        if qtd_clientes == 0:
            raise ValueError("Tabela de clientes está vazia!")
            
        cpfs_nulos = con.execute("SELECT count(*) FROM 'data/silver/clientes/*.parquet' WHERE cpf IS NULL").fetchone()[0]
        if cpfs_nulos > 0:
            raise ValueError(f"Encontrados {cpfs_nulos} clientes sem CPF!")
            
        con.close()
        return "Validações de Data Quality concluídas e aprovadas."

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
    processar_silver_padronizados() >> validar_qualidade_silver() >> processar_gold_agregacoes()