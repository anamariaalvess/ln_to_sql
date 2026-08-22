"""
Geração de dados sintéticos para demonstração do projeto
Natural Language to SQL.

Os dados são totalmente fictícios e simulam três contextos:
- faturamento;
- estoque;
- contas a receber.
"""

import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


# ============================================================
# CONFIGURAÇÕES
# ============================================================

random.seed(42)

BASE_DIR = Path(__file__).resolve().parent
DADOS_DIR = BASE_DIR / "dados"

DADOS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# DADOS DE REFERÊNCIA
# ============================================================

CLIENTES = [
    (1, "Alfa Comércio"),
    (2, "Beta Tecnologia"),
    (3, "Central Distribuidora"),
    (4, "Delta Serviços"),
    (5, "Elo Consultoria"),
    (6, "Fênix Varejo"),
    (7, "Gama Soluções"),
    (8, "Horizonte Comercial"),
    (9, "Integra Negócios"),
    (10, "Jota Empresas"),
]

PRODUTOS = [
    (101, "Notebook Pro", "Informática", 4200.00),
    (102, "Monitor 24", "Informática", 950.00),
    (103, "Teclado Mecânico", "Periféricos", 320.00),
    (104, "Mouse Sem Fio", "Periféricos", 150.00),
    (105, "Headset USB", "Periféricos", 280.00),
    (106, "Cadeira Executiva", "Mobiliário", 1250.00),
    (107, "Mesa Escritório", "Mobiliário", 980.00),
    (108, "Webcam HD", "Periféricos", 390.00),
    (109, "SSD 1TB", "Armazenamento", 520.00),
    (110, "Dock Station", "Acessórios", 680.00),
]

CANAIS = [
    "Loja física",
    "E-commerce",
    "Venda direta",
]

CIDADES = [
    "Goiânia",
    "Anápolis",
    "Aparecida de Goiânia",
    "Rio Verde",
    "Catalão",
]


# ============================================================
# FATURAMENTO
# ============================================================

def gerar_faturamento(
    quantidade_registros: int = 150,
) -> pd.DataFrame:
    """
    Gera dados fictícios de vendas e faturamento.
    """

    registros = []

    data_inicial = date(2026, 1, 1)

    for id_venda in range(1, quantidade_registros + 1):

        id_cliente, cliente = random.choice(CLIENTES)

        (
            id_produto,
            produto,
            categoria,
            valor_unitario,
        ) = random.choice(PRODUTOS)

        quantidade = random.randint(1, 10)

        desconto_percentual = random.choice(
            [0, 0, 0, 5, 10, 15]
        )

        valor_bruto = quantidade * valor_unitario

        valor_total = valor_bruto * (
            1 - desconto_percentual / 100
        )

        data_venda = data_inicial + timedelta(
            days=random.randint(0, 210)
        )

        registros.append(
            {
                "id_venda": id_venda,
                "data_venda": data_venda,
                "id_cliente": id_cliente,
                "cliente": cliente,
                "id_produto": id_produto,
                "produto": produto,
                "categoria": categoria,
                "quantidade": quantidade,
                "valor_unitario": valor_unitario,
                "desconto_percentual": desconto_percentual,
                "valor_total": round(valor_total, 2),
                "canal_venda": random.choice(CANAIS),
                "cidade": random.choice(CIDADES),
            }
        )

    return pd.DataFrame(registros)


# ============================================================
# ESTOQUE
# ============================================================

def gerar_estoque() -> pd.DataFrame:
    """
    Gera dados fictícios de estoque dos produtos.
    """

    registros = []

    fornecedores = [
        "Fornecedor Alfa",
        "Fornecedor Beta",
        "Fornecedor Central",
        "Fornecedor Delta",
    ]

    for (
        id_produto,
        produto,
        categoria,
        preco_venda,
    ) in PRODUTOS:

        quantidade_estoque = random.randint(5, 120)
        estoque_minimo = random.randint(10, 30)

        custo_unitario = preco_venda * random.uniform(
            0.55,
            0.75,
        )

        registros.append(
            {
                "id_produto": id_produto,
                "produto": produto,
                "categoria": categoria,
                "quantidade_estoque": quantidade_estoque,
                "estoque_minimo": estoque_minimo,
                "custo_unitario": round(
                    custo_unitario,
                    2,
                ),
                "preco_venda": preco_venda,
                "fornecedor": random.choice(
                    fornecedores
                ),
                "data_atualizacao": date(2026, 8, 1),
            }
        )

    return pd.DataFrame(registros)


# ============================================================
# CONTAS A RECEBER
# ============================================================

def gerar_contas_receber(
    quantidade_registros: int = 40,
) -> pd.DataFrame:
    """
    Gera dados fictícios de contas a receber.
    """

    registros = []

    data_referencia = date(2026, 8, 1)

    for id_titulo in range(1, quantidade_registros + 1):

        id_cliente, cliente = random.choice(CLIENTES)

        data_emissao = date(2026, 4, 1) + timedelta(
            days=random.randint(0, 120)
        )

        prazo = random.choice(
            [15, 30, 45, 60]
        )

        data_vencimento = data_emissao + timedelta(
            days=prazo
        )

        valor_original = round(
            random.uniform(500, 20000),
            2,
        )

        percentual_pago = random.choice(
            [0, 0, 0.25, 0.5, 1]
        )

        valor_pago = round(
            valor_original * percentual_pago,
            2,
        )

        valor_aberto = round(
            valor_original - valor_pago,
            2,
        )

        if valor_aberto == 0:
            status = "Pago"
            dias_atraso = 0

        elif data_vencimento < data_referencia:
            status = "Vencido"

            dias_atraso = (
                data_referencia - data_vencimento
            ).days

        else:
            status = "Em aberto"
            dias_atraso = 0

        registros.append(
            {
                "id_titulo": id_titulo,
                "id_cliente": id_cliente,
                "cliente": cliente,
                "data_emissao": data_emissao,
                "data_vencimento": data_vencimento,
                "valor_original": valor_original,
                "valor_pago": valor_pago,
                "valor_aberto": valor_aberto,
                "status": status,
                "dias_atraso": dias_atraso,
            }
        )

    return pd.DataFrame(registros)


# ============================================================
# SALVAMENTO
# ============================================================

def salvar_dados() -> None:
    """
    Gera e salva todos os conjuntos de dados em Parquet.
    """

    faturamento = gerar_faturamento()
    estoque = gerar_estoque()
    receber = gerar_contas_receber()

    faturamento.to_parquet(
        DADOS_DIR / "faturamento.parquet",
        index=False,
    )

    estoque.to_parquet(
        DADOS_DIR / "estoque.parquet",
        index=False,
    )

    receber.to_parquet(
        DADOS_DIR / "receber.parquet",
        index=False,
    )

    print("Dados sintéticos gerados com sucesso.")
    print()
    print(f"Faturamento: {len(faturamento)} registros")
    print(f"Estoque: {len(estoque)} registros")
    print(f"Contas a receber: {len(receber)} registros")
    print()
    print(f"Arquivos salvos em: {DADOS_DIR}")


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    salvar_dados()