from airflow.sdk import dag, task
from pathlib import Path
import duckdb


@dag("ddl_dag", start_date=datetime.now(), schedule=None)
def criador_de_tabelas():

    @task
    def criar_database():

        resposta = False

        if Path("sql/duckbank.duckdb").exists():
            print("Database ja criado.")
            resposta = True
            return resposta 
        else:
            con = duckdb.connect("sql/duckbank.duckdb")

            con.execute("CREATE TABLE sales (id INT, total FLOAT)")

            con.close()
            print("Database criado com sucesso.")
            resposta = True
            return resposta 

    @task.skip_if(criar_database() == False)
    def criar_tabelas():

        con = duckdb.connect("sql/duckbank.duckdb")

        try:
            con.execute(read.sql("sql/ddl_database.sql"))
        except Exception as e:
            print(e)
           
        con.close()
        return print("Tabelas criadas com sucesso.")




    criar_tabelas()


dag_pipeline = processador()