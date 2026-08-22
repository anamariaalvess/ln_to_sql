"""
Utilitário opcional para exportação de dados PostgreSQL
para o formato Parquet.

Este script não é necessário para executar a aplicação principal.
Ele demonstra uma forma simples de extrair dados de uma tabela
PostgreSQL e armazená-los localmente em formato Parquet.
"""

import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql


# ============================================================
# CONFIGURAÇÕES
# ============================================================

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DADOS_DIR = BASE_DIR / "dados"

DATABASE_URL = os.getenv("DATABASE_URL")

TABELA_ORIGEM = "view_app_contas_receber"
ARQUIVO_SAIDA = DADOS_DIR / "exportacao_postgres.parquet"


# ============================================================
# EXPORTAÇÃO
# ============================================================

def exportar_tabela_para_parquet() -> None:
    """
    Extrai uma tabela PostgreSQL e salva os dados
    em formato Parquet.
    """

    if not DATABASE_URL:
        raise ValueError(
            "A variável DATABASE_URL não foi configurada."
        )

    DADOS_DIR.mkdir(parents=True, exist_ok=True)

    conexao = psycopg2.connect(DATABASE_URL)

    try:
        cursor = conexao.cursor()

        consulta = sql.SQL(
            "SELECT * FROM {}"
        ).format(
            sql.Identifier(TABELA_ORIGEM)
        )

        cursor.execute(consulta)

        colunas = [
            descricao[0]
            for descricao in cursor.description
        ]

        dados = cursor.fetchall()

        dataframe = pd.DataFrame(
            dados,
            columns=colunas,
        )

        dataframe.to_parquet(
            ARQUIVO_SAIDA,
            index=False,
        )

        print(
            f"Exportação concluída: "
            f"{len(dataframe)} registros salvos em "
            f"{ARQUIVO_SAIDA}"
        )

    finally:
        conexao.close()


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    exportar_tabela_para_parquet()