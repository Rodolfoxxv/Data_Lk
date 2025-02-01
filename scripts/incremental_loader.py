import duckdb
import psycopg2
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

# Validar variáveis de ambiente
if not all([SUPABASE_URL, SUPABASE_KEY, DUCKDB_PATH, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")

# Resolver caminho do DuckDB
full_duckdb_path = Path("D:/Projetos/Data_Lk/data") / DUCKDB_PATH
full_duckdb_path = full_duckdb_path.resolve()

if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {full_duckdb_path}")

logger.info(f"DUCKDB_PATH: {full_duckdb_path}")

# Conectar ao DuckDB
conn_duckdb = duckdb.connect(str(full_duckdb_path))

# Função para criar tabela de controle
def criar_tabela_controle(cursor_supabase):
    tabela_controle = "controle_cargas"
    query_criar_tabela = f"""
    CREATE TABLE IF NOT EXISTS {tabela_controle} (
        tabela_nome TEXT PRIMARY KEY,
        ultima_carga TIMESTAMP,
        linhas_carregadas INT
    );
    """
    try:
        cursor_supabase.execute(query_criar_tabela)
        logger.info(f"Tabela de controle '{tabela_controle}' criada ou verificada.")
    except psycopg2.Error as e:
        logger.error(f"Erro ao criar tabela de controle: {e}")
        raise

# Função para processar cada tabela
def processar_tabela(nome_tabela):
    try:
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        ) as conn_supabase:
            with conn_supabase.cursor() as cursor_supabase:
                # Verificar se a tabela já foi carregada antes
                cursor_supabase.execute(
                    f"SELECT EXISTS(SELECT 1 FROM controle_cargas WHERE tabela_nome = %s)",
                    (nome_tabela,),
                )
                existe = cursor_supabase.fetchone()[0]

                if existe:
                    # Tabela já carregada: verificar alterações
                    cursor_supabase.execute(
                        f"SELECT ultima_carga, linhas_carregadas FROM controle_cargas WHERE tabela_nome = %s",
                        (nome_tabela,),
                    )
                    ultima_carga, linhas_carregadas = cursor_supabase.fetchone()
                    total_linhas = conn_duckdb.execute(f"SELECT COUNT(*) FROM {nome_tabela}").fetchone()[0]

                    if total_linhas > linhas_carregadas:
                        logger.info(f"Tabela {nome_tabela} tem novas linhas. Carregando...")
                        novos_dados = conn_duckdb.execute(
                            f"SELECT * FROM {nome_tabela} LIMIT {total_linhas - linhas_carregadas} OFFSET {linhas_carregadas}"
                        ).fetchall()

                        # Inserir novos dados
                        for linha in novos_dados:
                            colunas = conn_duckdb.execute(f"DESCRIBE {nome_tabela}").fetchall()
                            colunas_nomes = [col[0] for col in colunas]
                            query_inserir = f"INSERT INTO {nome_tabela} ({', '.join(colunas_nomes)}) VALUES ({', '.join(['%s'] * len(colunas_nomes))})"
                            cursor_supabase.execute(query_inserir, linha)
                        conn_supabase.commit()

                        # Atualizar tabela de controle
                        cursor_supabase.execute(
                            f"""
                            UPDATE controle_cargas
                            SET ultima_carga = %s, linhas_carregadas = %s
                            WHERE tabela_nome = %s
                            """,
                            (datetime.now(), total_linhas, nome_tabela),
                        )
                        conn_supabase.commit()
                    else:
                        logger.info(f"Tabela {nome_tabela} não teve alterações.")
                else:
                    # Tabela nova: carregar todos os dados
                    logger.info(f"Tabela {nome_tabela} é nova. Carregando...")
                    dados = conn_duckdb.execute(f"SELECT * FROM {nome_tabela}").fetchall()
                    colunas = conn_duckdb.execute(f"DESCRIBE {nome_tabela}").fetchall()
                    colunas_supabase = [f"{col[0]} {col[1]}" for col in colunas]

                    # Criar tabela no Supabase
                    query_criar_tabela_supabase = f"CREATE TABLE {nome_tabela} ({', '.join(colunas_supabase)})"
                    cursor_supabase.execute(query_criar_tabela_supabase)
                    conn_supabase.commit()

                    # Inserir dados no Supabase
                    for linha in dados:
                        colunas_nomes = [col[0] for col in colunas]
                        query_inserir = f"INSERT INTO {nome_tabela} ({', '.join(colunas_nomes)}) VALUES ({', '.join(['%s'] * len(colunas_nomes))})"
                        cursor_supabase.execute(query_inserir, linha)
                    conn_supabase.commit()

                    # Registrar na tabela de controle
                    cursor_supabase.execute(
                        f"""
                        INSERT INTO controle_cargas (tabela_nome, ultima_carga, linhas_carregadas)
                        VALUES (%s, %s, %s)
                        """,
                        (nome_tabela, datetime.now(), len(dados)),
                    )
                    conn_supabase.commit()

    except Exception as e:
        logger.error(f"Erro ao processar tabela {nome_tabela}: {e}")
        raise

# Função principal
def main():
    try:
        # Conectar ao Supabase e criar tabela de controle
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        ) as conn_supabase:
            with conn_supabase.cursor() as cursor_supabase:
                criar_tabela_controle(cursor_supabase)

        # Obter tabelas do DuckDB
        tabelas_duckdb = conn_duckdb.execute("SHOW TABLES").fetchall()
        nomes_tabelas_duckdb = [tabela[0] for tabela in tabelas_duckdb]

        # Processar tabelas em paralelo
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(processar_tabela, nome_tabela) for nome_tabela in nomes_tabelas_duckdb]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Erro durante a execução: {e}")

    except Exception as e:
        logger.error(f"Erro no pipeline: {e}")
        raise
    finally:
        # Fechar conexão com DuckDB
        conn_duckdb.close()
        logger.info("Pipeline concluído.")

# Executar pipeline
if __name__ == "__main__":
    main()