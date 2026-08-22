import json
import os
import re
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from openai import OpenAI


# ============================================================
# CONFIGURAÇÕES
# ============================================================

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DADOS_DIR = BASE_DIR / "dados"

MODELO_OPENAI = os.getenv("OPENAI_MODEL", "gpt-5.6-luna")

ARQUIVOS_DADOS = {
    "FATURAMENTO": DADOS_DIR / "faturamento.parquet",
    "ESTOQUE": DADOS_DIR / "estoque.parquet",
    "RECEBER": DADOS_DIR / "receber.parquet",
}


# ============================================================
# OPENAI
# ============================================================

client = OpenAI()


# ============================================================
# DUCKDB
# ============================================================

def criar_conexao() -> duckdb.DuckDBPyConnection:
    """
    Cria uma conexão DuckDB em memória e registra
    os arquivos Parquet como views.
    """

    arquivos_ausentes = [
        caminho.name
        for caminho in ARQUIVOS_DADOS.values()
        if not caminho.exists()
    ]

    if arquivos_ausentes:
        arquivos = ", ".join(arquivos_ausentes)

        raise FileNotFoundError(
            f"Arquivos de dados não encontrados: {arquivos}. "
            "Gere os dados sintéticos antes de executar a aplicação."
        )

    conexao = duckdb.connect(database=":memory:")

    for nome_view, caminho in ARQUIVOS_DADOS.items():
        caminho_sql = caminho.as_posix().replace("'", "''")

        conexao.execute(
            f"""
            CREATE OR REPLACE VIEW {nome_view} AS
            SELECT *
            FROM read_parquet('{caminho_sql}')
            """
        )

    return conexao


def obter_schema(conexao: duckdb.DuckDBPyConnection) -> list[dict]:
    """
    Retorna as tabelas, colunas e tipos de dados
    disponíveis no DuckDB.
    """

    consulta = """
        SELECT
            table_name,
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY
            table_name,
            ordinal_position
    """

    dataframe = conexao.execute(consulta).df()

    return dataframe.to_dict(orient="records")


# ============================================================
# TRATAMENTO DO SQL
# ============================================================

def limpar_sql(resposta: str) -> str:
    """
    Remove marcações Markdown eventualmente retornadas
    pelo modelo e valida se a resposta contém uma consulta.
    """

    sql = resposta.strip()

    sql = re.sub(
        r"^```(?:sql)?\s*",
        "",
        sql,
        flags=re.IGNORECASE,
    )

    sql = re.sub(
        r"\s*```$",
        "",
        sql,
    )

    sql = sql.strip()

    if not re.match(r"^(SELECT|WITH)\b", sql, re.IGNORECASE):
        raise ValueError(
            "O modelo não retornou uma consulta SQL de leitura válida."
        )

    return sql


# ============================================================
# GERAÇÃO DO SQL
# ============================================================

def gerar_sql(pergunta: str) -> str:
    """
    Recebe uma pergunta em linguagem natural e utiliza
    o schema dos dados como contexto para gerar uma
    consulta SQL.
    """

    if not pergunta or not pergunta.strip():
        raise ValueError("A pergunta não pode estar vazia.")

    conexao = criar_conexao()

    try:
        schema = obter_schema(conexao)
    finally:
        conexao.close()

    schema_json = json.dumps(
        schema,
        ensure_ascii=False,
        indent=2,
    )

    prompt = f"""
Considere o seguinte schema de dados disponível no DuckDB:

{schema_json}

Pergunta do usuário:

{pergunta.strip()}

Gere uma consulta SQL DuckDB que responda à pergunta.
Use exclusivamente as tabelas e colunas presentes no schema fornecido.
"""

    resposta = client.responses.create(
        model=MODELO_OPENAI,
        instructions=(
            "Você é um especialista em SQL e análise de dados. "
            "Sua tarefa é converter perguntas em linguagem natural "
            "em consultas SQL compatíveis com DuckDB. "
            "Responda exclusivamente com a consulta SQL, sem explicações, "
            "sem Markdown e sem comentários. "
            "Utilize somente operações de leitura, como SELECT e WITH. "
            "Nunca gere INSERT, UPDATE, DELETE, DROP, ALTER ou outras "
            "operações que modifiquem os dados."
        ),
        input=prompt,
    )

    if not resposta.output_text:
        raise RuntimeError(
            "O modelo não retornou uma resposta."
        )

    return limpar_sql(resposta.output_text)