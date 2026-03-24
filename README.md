# 🏦 Banking Data Pipeline

Pipeline de dados local com arquitetura medallion usando **Apache Airflow**, **PySpark** e **DuckDB** — construído como projeto de portfólio para consolidar conhecimentos em engenharia de dados.

---

## Motivação

Este projeto nasceu de uma decisão deliberada: **trocar complexidade de infraestrutura por profundidade em orquestração**.

A ideia original envolvia Azure Data Factory, ADLS Gen 2 e Iceberg com catálogo gerenciado — uma stack válida, mas que transfere grande parte da responsabilidade para serviços gerenciados. O resultado seria um pipeline funcional, porém com pouco aprendizado real sobre o que acontece por baixo.

A pergunta que guiou a reformulação foi: *o que um engenheiro de dados júnior/pleno realmente precisa demonstrar para se destacar?*

A resposta foi Airflow. É a ferramenta de orquestração mais exigida em vagas de dados no Brasil, e saber configurar uma DAG em produção — com dependências, retries, sensors e agendamento — é diferente de ter rodado exemplos de tutorial. Este projeto foi desenhado para cobrir exatamente isso, de forma que possa ser explicado em detalhes numa entrevista técnica.

O Spark está presente, mas como coadjuvante: o suficiente para mostrar familiaridade com transformações distribuídas sem desviar o foco. Os dados em cloud ficam cobertos por outros projetos no portfólio.

---

## O que este projeto demonstra

- Criação e estruturação de DAGs com a **TaskFlow API** (`@task` decorator)
- Uso de diferentes **Operators**: `PythonOperator`, `BashOperator`, `SparkSubmitOperator`
- **Sensors** para verificar disponibilidade de dados antes de processar (`FileSensor`)
- **Scheduling** com `@daily` e uso correto de `catchup=False`
- Passagem de estado entre tasks com **XCom**
- Separação clara de responsabilidades entre camadas (Bronze → Silver → Gold)
- Transformações com **PySpark**: limpeza, tipagem, particionamento e agregação
- Consulta analítica com **DuckDB** diretamente sobre arquivos Parquet
- Ambiente 100% containerizado com **Docker Compose** — reproduzível em qualquer máquina

---

## Arquitetura

```
Faker (gerador de dados)
        │
        ▼
  Bronze Layer          → JSON bruto particionado por data
  (PythonOperator)        data/bronze/<entidade>/date=YYYY-MM-DD/
        │
        ▼
  Silver Layer          → Parquet limpo, tipado e filtrado
  (SparkSubmitOperator)   data/silver/<entidade>/date=YYYY-MM-DD/
        │
        ▼
  Gold Layer            → Parquet agregado em Star Schema
  (SparkSubmitOperator)   data/gold/fct_transacoes/
                          data/gold/dim_cliente/
                          data/gold/dim_conta/
        │
        ▼
  Validação             → Consultas DuckDB verificam integridade da Gold
  (PythonOperator)
```

**Decisões de design:**

- **Parquet local em vez de Iceberg** — o catálogo Iceberg adiciona complexidade de infra que não agrega aprendizado de Airflow. DuckDB lê Parquet nativamente com performance equivalente para o volume do projeto.
- **MinIO removido** — object storage simulado em disco local é suficiente para demonstrar a arquitetura; a troca para S3/ADLS em produção seria apenas configuração de conexão.
- **Faker em vez de API externa** — permite controlar volume, simular incrementalidade real e eliminar dependências externas que poderiam quebrar uma demo.

---

## Stack

| Ferramenta | Versão | Papel |
|---|---|---|
| Apache Airflow | 2.9 | Orquestração e agendamento |
| Apache Spark | 3.5 | Transformações Bronze → Silver → Gold |
| DuckDB | 0.10 | Validação e consultas analíticas |
| Docker Compose | — | Ambiente local reproduzível |
| Python / Faker | 3.11 / 24.x | Geração dos dados fictícios |
| Parquet | — | Formato de armazenamento Silver e Gold |

---

## Dados

Domínio financeiro simulado com `Faker` em `pt_BR`, cobrindo cinco entidades:

| Entidade | Tipo | Volume |
|---|---|---|
| `clientes` | Base estática | 500 registros |
| `contas` | Base estática | 600 registros |
| `cartoes` | Base estática | 700 registros |
| `transacoes_pix` | Incremental diária | ~300 registros/dia |
| `transacoes_cartao` | Incremental diária | ~400 registros/dia |

A camada Gold organiza esses dados em um **Star Schema** com `fct_transacoes` como tabela fato e dimensões de cliente, conta e tempo.

---

## Como rodar

**Pré-requisitos:** Docker e Docker Compose instalados.

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/banking-data-pipeline
cd banking-data-pipeline

# 2. Suba o ambiente
docker compose up -d

# 3. Gere a carga inicial (últimos 30 dias)
docker compose exec airflow-worker python scripts/generate_data.py --full-load

# 4. Acesse o Airflow UI
# http://localhost:8080  (usuário: airflow / senha: airflow)
```

Ative a DAG `banking_pipeline` na interface e acompanhe a execução.

---

## Estrutura do repositório

```
.
├── dags/
│   └── banking_pipeline.py     # DAG principal
├── spark_jobs/
│   ├── bronze_to_silver.py     # Transformação Bronze → Silver
│   └── silver_to_gold.py       # Transformação Silver → Gold
├── scripts/
│   └── generate_data.py        # Gerador de dados com Faker
├── data/                        # Lakehouse local (gerado em runtime)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yml
└── README.md
```

---

## Outros projetos

Este projeto faz parte de um portfólio de engenharia de dados que cobre diferentes camadas da stack:

- **Azure End-to-End Pipeline** — ingestão com ADF, armazenamento em ADLS Gen 2, transformação com Databricks
- **Banking Data Pipeline** ← você está aqui — orquestração local com Airflow e Spark

---

> Dados gerados com [Faker](https://faker.readthedocs.io/). Nenhuma informação real foi utilizada.