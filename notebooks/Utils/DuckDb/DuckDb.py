import duckdb
import project_config
import pandas as pd
from datetime import datetime


class DuckDb(object):
    def __init__(self):
        self.__conn = self.__create_connection()


    def __create_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(project_config.DUCKDB_FILE_PATH)


    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return self.__conn
    

    def commit(self):
        self.__conn.execute('FORCE CHECKPOINT')
    

    def create_database_table(self, df: pd.DataFrame, table_name: str):
        self.__conn.register('df_view', df)
        self.__conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_view WHERE FALSE"
        )


    def upload_dataframe_to_duck_db(self, df: pd.DataFrame, table_name: str):
        print(f'{datetime.now()} Inserindo {len(df)} registros na tabela {table_name}')

        self.__conn.register('df', df)
        self.__conn.execute(
            f"INSERT INTO {table_name} SELECT * FROM df"
        )

        print(f'{datetime.now()} Inseridos com sucesso')


    def truncate_table(self, table_name: str):
        self.__conn.execute(
            f"TRUNCATE {table_name}"
        )


    def load_table(self, table_name: str, limit: int = 9_999_999_999) -> pd.DataFrame:
        query = f'''
            SELECT *
            FROM {table_name}
            LIMIT {limit}
        '''
        df = self.__conn.execute(query).fetch_df()
        return df
    

    def get_total_category_count(self, column: str, table: str) -> int:
        query = f'''
            SELECT
                MAX(CAST({column} AS INTEGER))
            FROM {table}
        '''
        result = self.__conn.execute(query).fetchone()
        return int(result[0])