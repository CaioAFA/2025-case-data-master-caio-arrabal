import duckdb
import project_config


class DuckDb(object):
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(project_config.DUCKDB_FILE_PATH)