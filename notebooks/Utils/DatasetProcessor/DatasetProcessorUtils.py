import pandas as pd
import project_config
from Utils.DuckDb.DuckDb import DuckDb
from typing import List
from datetime import datetime
from Utils.DataTransformer import DataTransformer


class DatasetProcessorUtils(object):
    def __init__(self):
        self.__duckdb = DuckDb()
        self.__data_transformer = DataTransformer()


    def load_table(self, table_name: str, limit: int = 9_999_999_999) -> pd.DataFrame:
        df = self.__duckdb.load_table(table_name, limit)
        return df
    

    def get_total_city_count(self) -> int:
        return self.__duckdb.get_total_category_count('city', project_config.DUCKDB_MEMBERS_DATABASE)


    def get_total_registered_via_count(self) -> int:
        return self.__duckdb.get_total_category_count('registered_via', project_config.DUCKDB_MEMBERS_DATABASE)


    def get_total_payment_method_id_count(self) -> int:
        return self.__duckdb.get_total_category_count('payment_method_id', project_config.DUCKDB_TRANSACTIONS_DATABASE)
    

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
    

    # TODO: add it on preprocessing step
    def calc_remaining_days(self, df: pd.DataFrame) -> pd.DataFrame:
        result = []
        for _, row in df.iterrows():
            transaction_date = datetime(row['transaction_date_year'], row['transaction_date_month'], row['transaction_date_day'])
            member_expire_date = datetime(row['membership_expire_date_year'], row['membership_expire_date_month'], row['membership_expire_date_day'])

            diff = (transaction_date - member_expire_date).total_seconds() / 24 / 60 / 60
            result.append(abs(diff))

        df['remaining_days'] = result
        return df
    

    def process_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        # Turning columns into categories
        cat_columns = {
            'payment_method_id': self.get_total_payment_method_id_count() + 1,
            'city': self.get_total_city_count() + 1,
            'registered_via': self.get_total_registered_via_count() + 1
        }

        df = self.__data_transformer.convert_to_category(
            df,
            list(cat_columns.keys())
        )

        df = pd.get_dummies(df, columns=list(cat_columns.keys()))

        # Filling out the missing categories
        for cat, total_count in cat_columns.items():
            for i in range(total_count):
                cat_key = f'{cat}_{int(i)}'

                # Fixing wrong values, like "city_1.0" -> "city_1"

                wrong_cat_key = f'{cat_key}.0'
                if wrong_cat_key in list(df.columns):
                    print(f'Ajustando categoria de nome "{wrong_cat_key}" para "{cat_key}"')
                    df = df.rename(columns={wrong_cat_key: cat_key})

                # print(f'Procurando {cat_key}...')

                if cat_key not in list(df.columns):
                    print(f'Adicionando coluna {cat_key}')
                    df[cat_key] = False

        return df
    

    def split_safras(self, df: pd.DataFrame) -> pd.DataFrame:
        df['safra_year'] = df['safra'].astype(str).str[:4].astype(float)
        df['safra_month'] = df['safra'].astype(str).str[-2:].astype(float)
        df = df.drop('safra', axis=1)
        return df