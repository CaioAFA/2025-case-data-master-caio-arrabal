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