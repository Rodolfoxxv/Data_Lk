import duckdb
import os
from dotenv import load_dotenv
from pathlib import Path

# Carregar variáveis de ambiente
load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH")

# Verificar se DUCKDB_PATH está definido
if DUCKDB_PATH is None:
    raise ValueError("DUCKDB_PATH não está definido. Verifique o arquivo .env.")

# Construir o caminho completo para o arquivo dentro da pasta 'data'
full_duckdb_path = Path('D:/Projetos/Data_Lk/data') / DUCKDB_PATH

# Converter o caminho relativo em absoluto
full_duckdb_path = full_duckdb_path.resolve()

# Verificar se o arquivo existe
if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"O arquivo {full_duckdb_path} não foi encontrado.")

# Exibir o valor de full_duckdb_path
print(f"DUCKDB_PATH: {full_duckdb_path}")

# Conectar ao banco de dados DuckDB
conn_duckdb = duckdb.connect(str(full_duckdb_path))

# Função para criar a tabela de teste
def create_test_table():
    conn_duckdb.execute("""
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR
    );
    """)
    print("Tabela de teste criada com sucesso.")

# Função para inserir dados de teste
def insert_test_data():
    conn_duckdb.execute("""
    INSERT INTO test_table (id, name) VALUES
    (1, 'Teste 1'),
    (2, 'Teste 2');
    """)
    print("Dados inseridos na tabela de teste.")

# Executar as funções
create_test_table()
insert_test_data()

# Fechar a conexão
conn_duckdb.close()