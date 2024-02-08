"""Este é um script para conectar no banco de dados e copiar uma tabela em formato parquet e salvar localmente."""

import psycopg2
import duckdb
import pandas as pd

# Conectar ao banco de dados PostgreSQL
DB_URL = "insert your db url where"
conn = psycopg2.connect(DB_URL)

# Nome da tabela a ser copiada
tabela_origem = "view_app_contas_receber"

# Consulta SQL para selecionar os dados da tabela
consulta_sql = f"SELECT * FROM {tabela_origem}"

# Criar conexão DuckDB
duckdb_conn = duckdb.connect()

# Executar a consulta SQL no PostgreSQL
cursor = conn.cursor()
cursor.execute(consulta_sql)

# Obter os dados do PostgreSQL como um DataFrame do pandas
dados = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

# Salvar os dados como um arquivo .parquet usando o DuckDB
duckdb_conn.register('tabela_origem', dados)
duckdb_conn.execute(r"COPY (SELECT * FROM tabela_origem) TO 'C:\Users\Inko\Desktop\IAASSISTANT\view_app_contas_receber.parquet' (FORMAT 'parquet')")

# Fechar as conexões
cursor.close()
conn.close()
duckdb_conn.close()