import duckdb
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
import psycopg2

# 1.env
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")


if DUCKDB_PATH is None:
    raise ValueError("DUCKDB_PATH não está definido. Verifique o arquivo .env.")


full_duckdb_path = Path('D:/Projetos/Data_Lk/data') / DUCKDB_PATH
full_duckdb_path = full_duckdb_path.resolve()


if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"O arquivo {full_duckdb_path} não foi encontrado.")


print(f"DUCKDB_PATH: {full_duckdb_path}")

# 2. DuckDB
conn_duckdb = duckdb.connect(str(full_duckdb_path))

# 3. Supabase (PostgreSQL)
conn_supabase = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
cursor_supabase = conn_supabase.cursor()

# 4. tabela de controle
tabela_controle = "controle_cargas"


query_criar_tabela = f"""
CREATE TABLE IF NOT EXISTS {tabela_controle} (
    tabela_nome TEXT PRIMARY KEY,
    ultima_carga TIMESTAMP,
    linhas_carregadas INT
);
"""
cursor_supabase.execute(query_criar_tabela)
conn_supabase.commit()

# 6.DuckDB
tabelas_duckdb = conn_duckdb.execute("SHOW TABLES").fetchall()
nomes_tabelas_duckdb = [tabela[0] for tabela in tabelas_duckdb]

# 7.carrega no Supabase
cursor_supabase.execute(f"SELECT tabela_nome FROM {tabela_controle}")
tabelas_supabase = cursor_supabase.fetchall()
nomes_tabelas_supabase = [tabela[0] for tabela in tabelas_supabase]

# 8. Deletadas no DuckDB
for tabela_supabase in nomes_tabelas_supabase:
    if tabela_supabase not in nomes_tabelas_duckdb:
        print(f"Tabela {tabela_supabase} foi apagada no DuckDB. Removendo do Supabase...")
        
        # Deleta tabela Supabase
        cursor_supabase.execute(f"DROP TABLE IF EXISTS {tabela_supabase}")
        
        # atualiza controle
        cursor_supabase.execute(f"DELETE FROM {tabela_controle} WHERE tabela_nome = %s", (tabela_supabase,))
        conn_supabase.commit()

# 9. carregar tabelas
for tabela in tabelas_duckdb:
    nome_tabela = tabela[0]
    print(f"Verificando tabela: {nome_tabela}")

    # Verificar se a tabela já foi carregada antes
    cursor_supabase.execute(f"SELECT * FROM {tabela_controle} WHERE tabela_nome = %s", (nome_tabela,))
    status_carga = cursor_supabase.fetchone()

    if status_carga:
        # verificar alterações
        ultima_carga = status_carga[1]
        linhas_carregadas = status_carga[2]

        # novas linhas
        total_linhas = conn_duckdb.execute(f"SELECT COUNT(*) FROM {nome_tabela}").fetchone()[0]
        if total_linhas > linhas_carregadas:
            print(f"Tabela {nome_tabela} tem novas linhas. Carregando...")
            dados = conn_duckdb.execute(f"SELECT * FROM {nome_tabela} LIMIT {total_linhas - linhas_carregadas} OFFSET {linhas_carregadas}").fetchall()
            for linha in dados:
                colunas = conn_duckdb.execute(f"DESCRIBE {nome_tabela}").fetchall()
                colunas_nomes = [col[0] for col in colunas]
                query_inserir = f"INSERT INTO {nome_tabela} ({', '.join(colunas_nomes)}) VALUES ({', '.join(['%s'] * len(colunas_nomes))})"
                cursor_supabase.execute(query_inserir, linha)
            conn_supabase.commit()

            # Atualizar tabela de controle
            cursor_supabase.execute(f"""
                UPDATE {tabela_controle}
                SET ultima_carga = %s, linhas_carregadas = %s
                WHERE tabela_nome = %s
            """, (datetime.now(), total_linhas, nome_tabela))
            conn_supabase.commit()
        else:
            print(f"Tabela {nome_tabela} não teve alterações.")
    else:
        # Tabela nova
        print(f"Tabela {nome_tabela} é nova. Carregando...")
        dados = conn_duckdb.execute(f"SELECT * FROM {nome_tabela}").fetchall()
        colunas = conn_duckdb.execute(f"DESCRIBE {nome_tabela}").fetchall()

        # Cria tabela no Supabase
        colunas_supabase = [f"{col[0]} {col[1]}" for col in colunas]
        query_criar_tabela_supabase = f"""
        CREATE TABLE {nome_tabela} (
            {", ".join(colunas_supabase)}
        );
        """
        cursor_supabase.execute(query_criar_tabela_supabase)
        conn_supabase.commit()

        # Inserir dados no Supabase
        for linha in dados:
            colunas_nomes = [col[0] for col in colunas]
            query_inserir = f"INSERT INTO {nome_tabela} ({', '.join(colunas_nomes)}) VALUES ({', '.join(['%s'] * len(colunas_nomes))})"
            cursor_supabase.execute(query_inserir, linha)
        conn_supabase.commit()

        # Registrar na tabela de controle
        cursor_supabase.execute(f"""
            INSERT INTO {tabela_controle} (tabela_nome, ultima_carga, linhas_carregadas)
            VALUES (%s, %s, %s)
        """, (nome_tabela, datetime.now(), len(dados)))
        conn_supabase.commit()

# 10. Fechar conexões
conn_duckdb.close()
cursor_supabase.close()
conn_supabase.close()