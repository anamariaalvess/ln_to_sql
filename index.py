import logging
import openai
from dotenv import load_dotenv
import pyarrow.parquet as pq
import duckdb
import json


load_dotenv()  
OPENAI_API_KEY = "insert where"
openai.api_key = OPENAI_API_KEY

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

duckdb.sql("CREATE VIEW FATURAMENTO AS SELECT * FROM read_parquet('faturamento.parquet')")
duckdb.sql("CREATE VIEW ESTOQUE AS SELECT * FROM read_parquet('estoque.parquet')")
duckdb.sql("CREATE VIEW RECEBER AS SELECT * FROM read_parquet('receber.parquet')")
duckdb.sql("COPY (SELECT table_name, column_name, data_type FROM information_schema.columns) to 'output.json'")
my_json = duckdb.sql("SELECT table_name, column_name, data_type FROM information_schema.columns").to_df().to_json(orient='records')

def description_tables() -> dict:
    return json.loads(duckdb.sql("SELECT table_name, column_name, data_type FROM information_schema.columns").to_df().to_json(orient='records'))

def interact_with_gpt(question):
    """Esta função tem como entrada a pergunta que será feita pelo usuário, gera o sql e retorna o select referente a pergunta."""
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system", 
                "content":  (
                    "You are a SQL assistant, you only have to answer with SQL queries, no other text, only SQL."
                )
            },
            {
                "role": "user", 
                "content": question, "name": "Prompt"
            }
        ]
    )
    answer = completion.choices[0].message.content
    return answer

def execute_sql_query(connection, sql_query):
    """Esta função executa a consulta sql na tabela"""
    result = connection.execute(sql_query)
    result_data = result.fetchall()
    return result_data

def askGpt(question):
    """Esta função recebe a pergunta do usuário e retorna o sql e o resulado da consulta sql na tabela desejada."""
    try:
        promptToGptText = f'Dado um banco de dados com as tabelas, colunas e tipo descritas no json a seguir: '
        promptToGptText += f'{description_tables()}'
        question_for_gpt = f"{description_tables()}. The query I need is: {question}. Make the necessary changes to the column data types so that the select always runs without errors."
        sql_query = interact_with_gpt(question_for_gpt)

        
        return  sql_query
    except duckdb.Error as error:
        logger.error("Erro ao executar a consulta SQL:", error)
        return None, str(error)
    except Exception as error:
        logger.error("Erro inesperado:", error)
        return None, "Erro inesperado ao executar a consulta SQL."