"""
Gerador de dados bancários fictícios para pipeline de dados.
Gera: clientes, contas, cartões, transações PIX e transações de cartão.

Uso:
    python generate_data.py                     # gera para hoje
    python generate_data.py --date 2024-01-15   # gera para data específica
    python generate_data.py --full-load         # carga inicial (últimos 30 dias)

Saída: data/bronze/<entidade>/date=YYYY-MM-DD/<entidade>.json
"""

import json
import random
import argparse
import hashlib
from pathlib import Path
from datetime import datetime, date, timedelta
from faker import Faker

fake = Faker("pt_BR")
random.seed(42)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("data/bronze")

N_CLIENTES = 500
N_CONTAS = 600          # alguns clientes têm mais de uma conta
N_CARTOES = 700         # algumas contas têm mais de um cartão
N_PIX_POR_DIA = 300
N_CARTAO_POR_DIA = 400

BANDEIRAS = ["Visa", "Mastercard", "Elo", "Hipercard"]
TIPOS_CONTA = ["corrente", "poupança", "salário"]
TIPOS_CHAVE_PIX = ["CPF", "email", "telefone", "aleatoria"]
STATUS_TRANSACAO = ["aprovada", "aprovada", "aprovada", "recusada", "pendente"]  # peso maior em aprovada
STATUS_CARTAO = ["ativo", "ativo", "ativo", "bloqueado", "cancelado"]
CATEGORIAS_CARTAO = [
    "supermercado", "restaurante", "posto_combustivel", "farmacia",
    "e-commerce", "vestuario", "eletronicos", "transporte", "saude", "educacao"
]

# ---------------------------------------------------------------------------
# Geradores de entidades base (estáticas — geradas uma vez)
# ---------------------------------------------------------------------------

def gerar_cpf_formatado(fake_instance) -> str:
    """Gera CPF no formato XXX.XXX.XXX-XX."""
    return fake_instance.cpf()


def gerar_clientes(n: int) -> list[dict]:
    clientes = []
    for i in range(1, n + 1):
        clientes.append({
            "id_cliente": f"CLI{i:06d}",
            "nome": fake.name(),
            "cpf": gerar_cpf_formatado(fake),
            "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "email": fake.email(),
            "telefone": fake.cellphone_number(),
            "cidade": fake.city(),
            "estado": fake.estado_sigla(),
            "cep": fake.postcode(),
            "data_cadastro": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "score_credito": random.randint(300, 1000),
        })
    return clientes


def gerar_contas(clientes: list[dict], n: int) -> list[dict]:
    contas = []
    ids_clientes = [c["id_cliente"] for c in clientes]

    for i in range(1, n + 1):
        id_cliente = random.choice(ids_clientes)
        contas.append({
            "id_conta": f"CONTA{i:07d}",
            "id_cliente": id_cliente,
            "agencia": f"{random.randint(1, 9999):04d}",
            "numero_conta": f"{random.randint(100000, 999999)}-{random.randint(0, 9)}",
            "tipo_conta": random.choice(TIPOS_CONTA),
            "saldo_inicial": round(random.uniform(0, 50000), 2),
            "limite_cheque_especial": round(random.choice([0, 500, 1000, 2000, 5000]), 2),
            "data_abertura": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "status": random.choices(["ativa", "inativa", "encerrada"], weights=[85, 10, 5])[0],
        })
    return contas


def gerar_cartoes(contas: list[dict], n: int) -> list[dict]:
    cartoes = []
    ids_contas_ativas = [c["id_conta"] for c in contas if c["status"] == "ativa"]

    for i in range(1, n + 1):
        id_conta = random.choice(ids_contas_ativas)
        cartoes.append({
            "id_cartao": f"CARD{i:07d}",
            "id_conta": id_conta,
            "bandeira": random.choice(BANDEIRAS),
            "ultimos_4_digitos": f"{random.randint(1000, 9999)}",
            "limite_total": round(random.choice([500, 1000, 2000, 3000, 5000, 10000, 15000]), 2),
            "limite_disponivel": None,  # calculado abaixo
            "data_expiracao": fake.date_between(start_date="today", end_date="+5y").strftime("%m/%Y"),
            "status": random.choices(["ativo", "bloqueado", "cancelado"], weights=[80, 15, 5])[0],
            "modalidade": random.choice(["crédito", "débito", "crédito e débito"]),
            "data_emissao": fake.date_between(start_date="-4y", end_date="today").isoformat(),
        })
        # limite disponível é entre 10% e 100% do limite total
        limite = cartoes[-1]["limite_total"]
        cartoes[-1]["limite_disponivel"] = round(random.uniform(0.1 * limite, limite), 2)

    return cartoes


# ---------------------------------------------------------------------------
# Geradores de transações (dinâmicas — geradas por data)
# ---------------------------------------------------------------------------

def gerar_chave_pix(tipo: str, cliente: dict) -> str:
    if tipo == "CPF":
        return cliente["cpf"]
    elif tipo == "email":
        return cliente["email"]
    elif tipo == "telefone":
        return cliente["telefone"]
    else:
        return fake.uuid4()


def gerar_transacoes_pix(
    contas: list[dict],
    clientes: list[dict],
    data: date,
    n: int,
) -> list[dict]:
    transacoes = []

    # monta mapa conta → cliente para chaves PIX
    mapa_conta_cliente = {}
    clientes_map = {c["id_cliente"]: c for c in clientes}
    for conta in contas:
        mapa_conta_cliente[conta["id_conta"]] = clientes_map.get(conta["id_cliente"], {})

    contas_ativas = [c["id_conta"] for c in contas if c["status"] == "ativa"]

    for i in range(1, n + 1):
        id_origem = random.choice(contas_ativas)
        id_destino = random.choice(contas_ativas)
        while id_destino == id_origem:
            id_destino = random.choice(contas_ativas)

        tipo_chave = random.choice(TIPOS_CHAVE_PIX)
        cliente_destino = mapa_conta_cliente.get(id_destino, {})
        chave = gerar_chave_pix(tipo_chave, cliente_destino) if cliente_destino else fake.uuid4()

        hora = fake.date_time_between(
            start_date=datetime.combine(data, datetime.min.time()),
            end_date=datetime.combine(data, datetime.max.time()),
        )

        transacoes.append({
            "id_transacao": f"PIX{data.strftime('%Y%m%d')}{i:06d}",
            "id_conta_origem": id_origem,
            "id_conta_destino": id_destino,
            "valor": round(random.uniform(1, 5000), 2),
            "chave_pix_destino": chave,
            "tipo_chave": tipo_chave,
            "descricao": random.choice([
                "Pagamento", "Transferência", "Reembolso",
                "Aluguel", "Mensalidade", "Venda", None
            ]),
            "status": random.choices(
                ["aprovada", "recusada", "pendente"],
                weights=[88, 8, 4]
            )[0],
            "timestamp": hora.isoformat(),
            "data_processamento": data.isoformat(),
        })

    return transacoes


def gerar_transacoes_cartao(
    cartoes: list[dict],
    data: date,
    n: int,
) -> list[dict]:
    transacoes = []
    cartoes_ativos = [c["id_cartao"] for c in cartoes if c["status"] == "ativo"]

    for i in range(1, n + 1):
        id_cartao = random.choice(cartoes_ativos)
        hora = fake.date_time_between(
            start_date=datetime.combine(data, datetime.min.time()),
            end_date=datetime.combine(data, datetime.max.time()),
        )

        valor = round(random.uniform(5, 3000), 2)
        parcelas = random.choices([1, 2, 3, 6, 12], weights=[50, 15, 15, 10, 10])[0]

        transacoes.append({
            "id_transacao": f"CARD{data.strftime('%Y%m%d')}{i:06d}",
            "id_cartao": id_cartao,
            "valor_total": valor,
            "parcelas": parcelas,
            "valor_parcela": round(valor / parcelas, 2),
            "estabelecimento": fake.company(),
            "categoria": random.choice(CATEGORIAS_CARTAO),
            "cidade_estabelecimento": fake.city(),
            "estado_estabelecimento": fake.estado_sigla(),
            "modalidade": random.choice(["crédito", "débito"]),
            "status": random.choices(
                ["aprovada", "recusada", "pendente", "estornada"],
                weights=[82, 10, 4, 4]
            )[0],
            "timestamp": hora.isoformat(),
            "data_processamento": data.isoformat(),
        })

    return transacoes


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------

def salvar_json(dados: list[dict], entidade: str, data: date) -> Path:
    """Salva lista de dicts como JSON no padrão Hive partitioning."""
    pasta = OUTPUT_DIR / entidade 
    pasta.mkdir(parents=True, exist_ok=True)
    caminho = pasta / f"{entidade}{(datetime.now()).strftime("%Y-%m-%d-%H-%M-%S")}.json"
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    return caminho


def salvar_entidades_base(clientes, contas, cartoes):
    """Entidades base ficam em uma partição fixa (não particionada por data)."""
    data_hoje = date.today()
    p1 = salvar_json(clientes, "clientes", data_hoje)
    p2 = salvar_json(contas, "contas", data_hoje)
    p3 = salvar_json(cartoes, "cartoes", data_hoje)
    print(f"  ✓ clientes     → {p1}  ({len(clientes)} registros)")
    print(f"  ✓ contas       → {p2}  ({len(contas)} registros)")
    print(f"  ✓ cartoes      → {p3}  ({len(cartoes)} registros)")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def gerar_para_data(data: date, clientes, contas, cartoes, verbose=True):
    pix = gerar_transacoes_pix(contas, clientes, data, N_PIX_POR_DIA)
    cartao_trans = gerar_transacoes_cartao(cartoes, data, N_CARTAO_POR_DIA)

    p1 = salvar_json(pix, "transacoes_pix", data)
    p2 = salvar_json(cartao_trans, "transacoes_cartao", data)

    if verbose:
        print(f"  ✓ transacoes_pix    → {p1}  ({len(pix)} registros)")
        print(f"  ✓ transacoes_cartao → {p2}  ({len(cartao_trans)} registros)")


def main():
    parser = argparse.ArgumentParser(description="Gerador de dados bancários fictícios")
    parser.add_argument("--date", type=str, help="Data no formato YYYY-MM-DD (padrão: hoje)")
    parser.add_argument("--full-load", action="store_true", help="Gera dados dos últimos 30 dias")
    parser.add_argument("--days", type=int, default=30, help="Número de dias para --full-load (padrão: 30)")
    args = parser.parse_args()

    print("\n🏦 Gerador de Dados Bancários Fictícios")
    print("=" * 50)

    # Gera entidades base (sempre)
    print("\n📦 Gerando entidades base...")
    clientes = gerar_clientes(N_CLIENTES)
    contas = gerar_contas(clientes, N_CONTAS)
    cartoes = gerar_cartoes(contas, N_CARTOES)
    salvar_entidades_base(clientes, contas, cartoes)

    # Gera transações
    if args.full_load:
        print(f"\n📅 Full load — gerando transações para os últimos {args.days} dias...")
        hoje = date.today()
        datas = [hoje - timedelta(days=i) for i in range(args.days - 1, -1, -1)]
        for d in datas:
            print(f"\n  [{d.isoformat()}]")
            gerar_para_data(d, clientes, contas, cartoes)
    else:
        target_date = date.fromisoformat(args.date) if args.date else date.today()
        print(f"\n📅 Gerando transações para {target_date.isoformat()}...")
        gerar_para_data(target_date, clientes, contas, cartoes)

    print("\n✅ Concluído! Estrutura gerada em:", OUTPUT_DIR.resolve())
    print()


if __name__ == "__main__":
    main()
