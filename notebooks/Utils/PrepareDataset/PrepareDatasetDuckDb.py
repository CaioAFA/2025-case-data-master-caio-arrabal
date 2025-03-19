import pandas as pd
import project_config
from Utils.DuckDb.DuckDb import DuckDb
from typing import List


class PrepareDatasetDuckDbUtils(object):
    def __init__(self):
        self.__duckdb = DuckDb()


    def load_table(self, table_name: str, limit: int = 9_999_999_999) -> pd.DataFrame:
        conn = self.__duckdb.get_connection()
        query = f'''
            SELECT *
            FROM {table_name}
            LIMIT {limit}
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
        conn = self.__duckdb.get_connection()
        query = f'''
            SELECT
                MAX(CAST({column} AS INTEGER))
            FROM {table}
        '''
        result = conn.execute(query).fetchone()
        return int(result[0])
    

    def get_user_list(self, limit: int = 9999999999, offset: int = 0) -> List[str]:
        conn = self.__duckdb.get_connection()
        query = '''
            SELECT DISTINCT(msno)
            FROM main.user_logs ul
            ORDER BY msno
            LIMIT ?
            OFFSET ?
        '''
        query_results = conn.execute(query, [limit, offset]).fetchall()
        result = list(
            map(
                lambda qr: qr[0], query_results
            )
        )
        return result
    

    def get_dataset_by_users(self, msnos: List[str]) -> pd.DataFrame:
        query = '''
            SELECT
                ----------------------
                -- Calculated fields --
                ----------------------
                50 + (0.0051 * num_unq) + (0.0001 * ul.total_secs) AS cost,
                --	t.actual_amount_paid - cost AS net_profit,
                ----------------------
                -- User Logs fields --
                ----------------------
                ul.msno,
                ul.safra,
                ul.num_25,
                ul.num_50,
                ul.num_75,
                ul.num_985,
                ul.num_100,
                ul.num_unq,
                ul.total_secs,
                ul.total_hours,
                -------------------------
                -- Transactions fields --
                -------------------------
                t.msno,
                t.payment_method_id,
                t.payment_plan_days,
                t.plan_list_price,
                t.actual_amount_paid,
                t.is_auto_renew,
                t.is_cancel,
                t.safra,
                t.transaction_date_year,
                t.transaction_date_month,
                t.transaction_date_day,
                t.transaction_date_day_of_week,
                t.transaction_date_day_of_year,
                t.membership_expire_date_year,
                t.membership_expire_date_month,
                t.membership_expire_date_day,
                t.membership_expire_date_day_of_week,
                t.membership_expire_date_day_of_year,
                t.discount,
                t.price_per_month,
                ---------------------
                -- Members columns --
                ---------------------
                m.msno as members_msno,
                m.safra as members_safra,
                m.city,
                m.registered_via,
                m.is_active,
                m.registration_init_time_year,
                m.registration_init_time_month,
                m.registration_init_time_day,
                m.registration_init_time_day_of_week,
                m.registration_init_time_day_of_year
            FROM
                main.user_logs ul
            INNER JOIN
                main.transactions t ON
                t.msno == ul.msno
                AND t.safra == ul.safra
            LEFT JOIN
                main.members m ON
                m.msno = ul.msno AND m.safra = ul.safra
            WHERE
                ul.msno IN ?
            ORDER BY
                ul.msno,
                ul.safra
        '''

        conn = self.__duckdb.get_connection()
        query_results = conn.execute(query, (msnos,)).fetch_df()
        return query_results
    


    def get_complete_dataset(self, min_users_to_consider: int = 50_000, batch_size: int = 50_000) -> pd.DataFrame:
        all_dfs: List[pd.DataFrame] = []

        count = 0
        while count < min_users_to_consider:
            print(f'Processando count: {count}')

            users_msno = self.get_user_list(limit=batch_size, offset=count)
            count += batch_size

            all_dfs.append(
                self.get_dataset_by_users(users_msno)
            )

        print(f'Qtd. de dataframes: {len(all_dfs)}')

        all_dfs = list(
            filter(
                lambda df: df.__len__() > 0, all_dfs
            )
        )

        print(f'Qtd. de dataframes pós remoção dos vazios: {len(all_dfs)}')

        result = pd.concat(all_dfs)
        return result