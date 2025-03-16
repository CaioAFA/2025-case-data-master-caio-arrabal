import duckdb
import project_config
import pandas as pd


class DuckDb(object):
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(project_config.DUCKDB_FILE_PATH)
    

    def load_table(self, table_name: str) -> pd.DataFrame:
        conn = self.get_connection()
        query = f'''
            SELECT *
            FROM {table_name}
        '''
        df = conn.execute(query).fetch_df()
        return df
    

    def get_total_city_count(self) -> int:
        return self.__get_total_category_count('city', project_config.DUCKDB_MEMBERS_DATABASE)


    def get_total_registered_via_count(self) -> int:
        return self.__get_total_category_count('registered_via', project_config.DUCKDB_MEMBERS_DATABASE)


    def get_total_payment_method_id_count(self) -> int:
        return self.__get_total_category_count('payment_method_id', project_config.DUCKDB_TRANSACTIONS_DATABASE)


    def __get_total_category_count(self, column: str, table: str) -> int:
        conn = self.get_connection()
        query = f'''
            SELECT
                COUNT(DISTINCT({column}))
            FROM {table}
        '''
        result = conn.execute(query).fetchone()
        return result[0]