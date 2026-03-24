from airflow.sdk import dag, task
from airflow.sensors.base import PokeReturnValue


@dag(dag_id="pokemon2000")
def user_processing():

    @task
    def validar_tabelas(melhor_pokemon):
        pass

    @task
    def escrever_arquivos_padronizados():
        pass
    
    @task.sensor
    def checkar_arquivos_pasta_origem():
        pass
    
    @task
    def gerar_dados_falsos():
        pass

user_processing()
