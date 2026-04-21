-- =============================================================
-- DDL - Bank CRM Tables
-- =============================================================
CREATE TABLE IF NOT EXISTS cartoes_cred_debt (
    id_cartao VARCHAR,
    id_conta VARCHAR,
    bandeira SMALLINT,
    ultimos_4_digitos INTEGER,
    limite_total DECIMAL(17, 4),
    limite_disponivel DECIMAL(17, 4),
    data_expiracao DATE,
    status SMALLINT,
    modalidade TINYINT,
    data_emissao DATE,
    timestamp_ingestao TIMESTAMP
);

COMMENT ON TABLE cartoes_cred_debt IS 'Tabela contem informações de cartoes, limites e status atualizados.';

-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clientes_cadastrados (
    id_cliente VARCHAR,
    nome_cliente VARCHAR,
    cpf VARCHAR,
    data_nascimento DATE,
    email VARCHAR,
    telefone INTEGER,
    cidade VARCHAR,
    estado VARCHAR,
    cep INTEGER,
    data_cadastro DATE,
    score_credito INTEGER,
    timestamp_ingestao TIMESTAMP
);

COMMENT ON TABLE clientes_cadastrados IS 'Tabela contem informações de cadastro do cliente no banco.';

-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contas_clientes (
    id_conta VARCHAR,
    id_cliente VARCHAR,
    agencia_conta INTEGER,
    numero_conta INTEGER,
    tipo_conta SMALLINT,
    saldo_inicial DECIMAL(17, 4),
    cheque_especial_status TINYINT,
    limite_cheque_especial DECIMAL(17, 4),
    data_abertura DATE,
    status SMALLINT,
    timestamp_ingestao TIMESTAMP
);

COMMENT ON TABLE contas_clientes IS 'Tabela contem informações relaciaonados a contas no banco.';


CREATE TABLE IF NOT EXISTS transacoes_cred_debt_pix (
    id_transacao VARCHAR,
    id_cartao VARCHAR,
    id_conta_origem VARCHAR,
    id_conta_destino VARCHAR,
    chave_pix_destino VARCHAR,
    tipo_chave_pix_destino VARCHAR,
    descricao_pix VARCHAR,
    valor_total_transacao DECIMAL(17,4),
    parcelas INTEGER,
    valor_parcela DECIMAL(17,4),
    estabelecimento CHAR(5),
    categoria VARCHAR,
    cidade_estabelecimento VARCHAR,
    estado_estabelecimento VARCHAR,
    modalidade VARCHAR,
    status TINYINT,
    timestamp_transacao TIMESTAMP,
    data_processamento DATE,
    timestamp_ingestao TIMESTAMP
);

COMMENT ON TABLE contas_clientes IS 'Tabela contem informações de status de transacoes no banco.';