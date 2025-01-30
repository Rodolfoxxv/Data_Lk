import duckdb
import config

con = duckdb.connect(config.DB_PATH)

def create_test_table():
    con.execute("""
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR
    );
    """)
    print("Tabela de teste criada com sucesso.")

def insert_test_data():
    con.execute("""
    INSERT INTO test_table (id, name) VALUES
    (1, 'Teste 1'),
    (2, 'Teste 2');
    """)
    print("Dados inseridos na tabela de teste.")

create_test_table()
insert_test_data()

con.close()
