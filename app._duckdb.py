import streamlit as st
import pandas as pd
import plotly.express as px
import json
import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

# ========= Configuração do Ambiente =========
load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
if DUCKDB_PATH is None:
    st.error("DUCKDB_PATH não está definido no arquivo .env")
    st.stop()

full_duckdb_path = Path('D:/Projetos/Data_Lk/data') / DUCKDB_PATH
full_duckdb_path = full_duckdb_path.resolve()

# ========= Pipeline do DuckDB =========
class DuckDBPipeline:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.create_metadata_table()

    def create_metadata_table(self):
        """Cria a tabela de metadados usando table_name como PRIMARY KEY."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS table_metadata (
                table_name VARCHAR PRIMARY KEY,
                schema_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def create_table_dynamic(self, table_name: str, fields: list):
        """Cria tabelas dinamicamente com base em um schema fornecido."""
        existing = self.conn.execute(
            "SELECT table_name FROM table_metadata WHERE table_name = ?;",
            (table_name,)
        ).fetchone()
        if existing:
            raise ValueError(f"A tabela '{table_name}' já existe na pipeline.")

        column_defs = []
        schema_for_metadata = {}

        for field in fields:
            col_name = field.get("name")
            if not col_name:
                raise ValueError("Cada campo deve ter um 'name' definido.")
           
            data_type = field.get("data_type", "").strip() or "VARCHAR"
            col_def = f"{col_name} {data_type}"
            schema_for_metadata[col_name] = {"data_type": data_type}

            if field.get("primary_key", False):
                col_def += " PRIMARY KEY"
                schema_for_metadata[col_name]["primary_key"] = True
            else:
                schema_for_metadata[col_name]["primary_key"] = False

            fk = field.get("foreign_key")
            if fk:
                fk_table, fk_column = fk.get("table"), fk.get("column")
                if fk_table and fk_column:
                    col_def += f" REFERENCES {fk_table}({fk_column})"
                    schema_for_metadata[col_name]["foreign_key"] = {"table": fk_table, "column": fk_column}
                else:
                    raise ValueError(f"Definição de chave estrangeira para a coluna '{col_name}' está incompleta.")
            else:
                schema_for_metadata[col_name]["foreign_key"] = None

            column_defs.append(col_def)

        sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)});"
        self.conn.execute(sql)
        self.conn.execute(
            "INSERT INTO table_metadata (table_name, schema_json) VALUES (?, ?);",
            (table_name, json.dumps(schema_for_metadata))
        )

    def insert_data(self, table_name: str, data: dict):
        """Insere dados na tabela garantindo que a chave primária não seja duplicada.
        Se o campo 'id' for chave primária e não for fornecido, ele será atribuído automaticamente."""
        metadata = self.get_table_metadata(table_name)
        primary_keys = [col for col, info in metadata.items() if info.get("primary_key")]

        if primary_keys:
            
            if "id" in primary_keys and "id" not in data:
                max_id = self.conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name};").fetchone()[0]
                data["id"] = max_id + 1
            else:
                
                for pk in primary_keys:
                    if pk not in data:
                        st.error(f"O valor para a chave primária '{pk}' não foi fornecido.")
                        return

            
            pk_conditions = " AND ".join([f"{pk} = ?" for pk in primary_keys])
            pk_values = tuple(data[pk] for pk in primary_keys)
            exists = self.conn.execute(
                f"SELECT 1 FROM {table_name} WHERE {pk_conditions} LIMIT 1;",
                pk_values
            ).fetchone()
            if exists:
                st.error(f"Erro: O registro com chave primária {primary_keys} = {pk_values} já existe.")
                return

        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
        self.conn.execute(sql, tuple(data.values()))
        st.success("Dados inseridos com sucesso!")

    def update_data(self, table_name: str, set_data: dict, where_clause: str, where_params: tuple):
        set_str = ", ".join([f"{col} = ?" for col in set_data.keys()])
        sql = f"UPDATE {table_name} SET {set_str} WHERE {where_clause};"
        values = tuple(set_data.values()) + where_params
        self.conn.execute(sql, values)

    def delete_data(self, table_name: str, where_clause: str, where_params: tuple):
        sql = f"DELETE FROM {table_name} WHERE {where_clause};"
        self.conn.execute(sql, where_params)

    def delete_table(self, table_name: str):
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        self.conn.execute("DELETE FROM table_metadata WHERE table_name = ?;", (table_name,))

    def alter_table_add_column(self, table_name: str, column_name: str, data_type: str):
        self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type};")
        metadata = self.get_table_metadata(table_name)
        metadata[column_name] = {"data_type": data_type, "primary_key": False, "foreign_key": None}
        self.update_metadata(table_name, metadata)

    def alter_table_drop_column(self, table_name: str, column_name: str):
        self.conn.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name};")
        metadata = self.get_table_metadata(table_name)
        metadata.pop(column_name, None)
        self.update_metadata(table_name, metadata)

    def get_table_metadata(self, table_name: str):
        row = self.conn.execute(
            "SELECT schema_json FROM table_metadata WHERE table_name = ?;",
            (table_name,)
        ).fetchone()
        return json.loads(row[0]) if row else {}

    def update_metadata(self, table_name: str, metadata: dict):
        self.conn.execute(
            "UPDATE table_metadata SET schema_json = ? WHERE table_name = ?;",
            (json.dumps(metadata), table_name)
        )

    def list_tables(self):
        rows = self.conn.execute("SELECT table_name FROM table_metadata;").fetchall()
        return [r[0] for r in rows]

    def get_table_data(self, table_name: str):
        return self.conn.execute(f"SELECT * FROM {table_name};").fetchdf()

    def close(self):
        self.conn.close()


pipeline = DuckDBPipeline(str(full_duckdb_path))

# ========= Interface Streamlit =========
st.title("Interface Dinâmica para Gerenciamento de Tabelas (DuckDB Pipeline)")

# Menu lateral
operation = st.sidebar.radio("Selecione a operação", 
                             ("Criar Tabela", "Inserir Dados", "Atualizar Dados", "Deletar Dados", 
                              "Dropar Tabela", "Visualizar Tabela", "Listar Tabelas"))

# ----- CRIAR TABELA -----
if operation == "Criar Tabela":
    st.header("Criar Tabela")
    create_mode = st.radio("Modo de Criação", ("Formulário Manual", "Colar JSON do Schema"))
    table_name = st.text_input("Nome da Tabela", key="create_table_name")
    
    if create_mode == "Formulário Manual":
        st.subheader("Defina os campos da tabela")
        if "fields" not in st.session_state:
            st.session_state.fields = []

        if st.button("Adicionar Campo"):
            st.session_state.fields.append({"name": "", "data_type": "", "primary_key": False,
                                            "foreign_key_table": "", "foreign_key_column": ""})

        for idx, field in enumerate(st.session_state.fields):
            st.markdown(f"**Campo {idx+1}**")
            field["name"] = st.text_input(f"Nome do campo {idx+1}", value=field["name"], key=f"name_{idx}")
            field["data_type"] = st.text_input(f"Tipo (ou deixe em branco para VARCHAR) do campo {idx+1}", value=field["data_type"], key=f"type_{idx}")
            field["primary_key"] = st.checkbox(f"Chave Primária? (campo {idx+1})", value=field["primary_key"], key=f"pk_{idx}")
            col1, col2 = st.columns(2)
            with col1:
                field["foreign_key_table"] = st.text_input(f"Tabela FK (opcional) campo {idx+1}", value=field["foreign_key_table"], key=f"fk_table_{idx}")
            with col2:
                field["foreign_key_column"] = st.text_input(f"Coluna FK (opcional) campo {idx+1}", value=field["foreign_key_column"], key=f"fk_column_{idx}")

        if st.button("Criar Tabela"):
            try:
                fields = []
                for field in st.session_state.fields:
                    f = {
                        "name": field["name"],
                        "data_type": field["data_type"],
                        "primary_key": field["primary_key"]
                    }
                    if field["foreign_key_table"] and field["foreign_key_column"]:
                        f["foreign_key"] = {"table": field["foreign_key_table"], "column": field["foreign_key_column"]}
                    fields.append(f)
                pipeline.create_table_dynamic(table_name, fields)
                st.success(f"Tabela '{table_name}' criada com sucesso!")
                st.session_state.fields = []  # Limpa os campos
            except Exception as e:
                st.error(f"Erro: {e}")
    
    else:  # JSON
        st.subheader("Cole o schema JSON")
        json_schema = st.text_area("Schema JSON (deve ser uma lista de campos)", height=200)
        if st.button("Criar Tabela (JSON)"):
            try:
                fields = json.loads(json_schema)
                pipeline.create_table_dynamic(table_name, fields)
                st.success(f"Tabela '{table_name}' criada com sucesso!")
            except Exception as e:
                st.error(f"Erro: {e}")

# ----- INSERIR DADOS -----
elif operation == "Inserir Dados":
    st.header("Inserir Dados")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables)
        metadata = pipeline.get_table_metadata(table_name)
        st.write("Schema:", metadata)
        with st.form("insert_form"):
            data = {}
            for col in metadata.keys():
                data[col] = st.text_input(f"Valor para '{col}'")
            submitted = st.form_submit_button("Inserir")
            if submitted:
                pipeline.insert_data(table_name, data)

# ----- ATUALIZAR DADOS -----
elif operation == "Atualizar Dados":
    st.header("Atualizar Dados")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables, key="update_table")
        metadata = pipeline.get_table_metadata(table_name)
        st.write("Schema:", metadata)
        with st.form("update_form"):
            st.subheader("Novos Valores")
            set_data = {}
            for col in metadata.keys():
                set_data[col] = st.text_input(f"Novo valor para '{col}' (deixe em branco para não atualizar)", key=f"upd_{col}")
            where_clause = st.text_input("Cláusula WHERE (ex.: id = ?)", key="upd_where")
            where_value = st.text_input("Valor para a cláusula WHERE", key="upd_where_value")
            submitted = st.form_submit_button("Atualizar")
            if submitted:
                set_data = {col: val for col, val in set_data.items() if val != ''}
                if not set_data or where_clause == '' or where_value == '':
                    st.error("Preencha os campos de atualização e condição.")
                else:
                    pipeline.update_data(table_name, set_data, where_clause, (where_value,))
                    st.success("Dados atualizados com sucesso!")

# ----- DELETAR DADOS -----
elif operation == "Deletar Dados":
    st.header("Deletar Dados")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables, key="delete_table")
        with st.form("delete_form"):
            where_clause = st.text_input("Cláusula WHERE (ex.: id = ?)", key="del_where")
            where_value = st.text_input("Valor para a cláusula WHERE", key="del_where_value")
            submitted = st.form_submit_button("Deletar")
            if submitted:
                if where_clause == '' or where_value == '':
                    st.error("Preencha a condição de deleção.")
                else:
                    pipeline.delete_data(table_name, where_clause, (where_value,))
                    st.success("Dados deletados com sucesso!")

# ----- DROPAR TABELA -----
elif operation == "Dropar Tabela":
    st.header("Dropar Tabela")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela para dropar", tables, key="drop_table")
        if st.button("Dropar Tabela"):
            try:
                pipeline.delete_table(table_name)
                st.success(f"Tabela '{table_name}' removida com sucesso!")
            except Exception as e:
                st.error(f"Erro: {e}")

# ----- VISUALIZAR TABELA -----
elif operation == "Visualizar Tabela":
    st.header("Visualizar Dados da Tabela")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables, key="view_table")
        try:
            df = pipeline.get_table_data(table_name)
            st.dataframe(df)
            num_cols = df.select_dtypes(include='number').columns.tolist()
            cat_cols = df.select_dtypes(include='object').columns.tolist()
            if num_cols and cat_cols:
                st.subheader("Gráfico de Dispersão (Exemplo)")
                fig = px.scatter(df, x=cat_cols[0], y=num_cols[0], title=f"{table_name} - {cat_cols[0]} vs {num_cols[0]}")
                st.plotly_chart(fig)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")

# ----- LISTAR TABELAS -----
elif operation == "Listar Tabelas":
    st.header("Listar Tabelas")
    tables = pipeline.list_tables()
    st.write(tables)

st.sidebar.info("Interface integrada com a pipeline do DuckDB.")
