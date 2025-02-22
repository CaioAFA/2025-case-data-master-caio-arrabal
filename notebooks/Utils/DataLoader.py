import pandas as pd
from typing import List, Union, Iterator
from Utils.DataTransformer import DataTransformer
import pyarrow.parquet as pq


class DataLoader(object):
    def __init__(self):
        self.__data_transformer = DataTransformer()
        self.__transactions_df_file_name = '../data/transactions.parquet'


    def load_transactions_df(self) -> pd.DataFrame:
        transactions_df = pd.read_parquet(self.__transactions_df_file_name)
        transactions_df = self.__process_load_transactions_df(transactions_df)
        return transactions_df


    def load_transactions_df_in_chunks(self, chunksize: int = None) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        parquet_file = pq.ParquetFile(self.__transactions_df_file_name)
        for batch in parquet_file.iter_batches(batch_size=chunksize):
            chunk = batch.to_pandas()
            chunk = self.__process_load_transactions_df(chunk)
            yield chunk


    def __process_load_transactions_df(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_fields = [
            'transaction_date',
            'membership_expire_date'
        ]
        df = self.__data_transformer.convert_to_datetime(df, dt_fields)
        df = self.__data_transformer.split_datetime_field_in_year_month_and_day(df, dt_fields)

        df = self.__data_transformer.convert_to_integer(df, [
            'payment_method_id',
            'payment_plan_days',
            'plan_list_price',
            'actual_amount_paid',

            # Transform into int, to transform into bool
            'is_auto_renew',
            'is_cancel'
        ])

        df = self.__data_transformer.convert_to_boolean(df, [
            'is_auto_renew',
            'is_cancel'
        ])

        df['discount'] = df['plan_list_price'] - df['actual_amount_paid']

        # Remove negative values
        df['discount'] = df['discount'].apply(lambda x: x if x >= 0 else 0)

        df['price_per_month'] = df['actual_amount_paid'] / (df['payment_plan_days'] / 30)

        # Unused field
        # df.drop('safra', axis=1, inplace=True)

        # Outliers
        df = df[df['membership_expire_date_year'] >= 2015]

        return df
    

    def load_members_df(self) -> pd.DataFrame:
        members_df = pd.read_parquet(
            '../data/members.parquet'
        )

        dt_fields = ['registration_init_time']
        members_df = self.__data_transformer.convert_to_datetime(members_df, dt_fields)
        members_df = self.__data_transformer.split_datetime_field_in_year_month_and_day(members_df, dt_fields)

        members_df = self.__data_transformer.convert_to_category(members_df, [
            'city',
            'registered_via'
        ])

        members_df = self.__data_transformer.convert_to_boolean(members_df, [
            'is_ativo'
        ])

        members_df.rename({
            'is_ativo': 'is_active'
        }, inplace=True, axis=1)

        members_df.drop('gender', axis=1, inplace=True)
        members_df.drop('bd', axis=1, inplace=True)

        return members_df
    

    def load_user_logs_df(self) -> pd.DataFrame:
        user_logs_df = pd.read_parquet('../data/user_logs.parquet')

        user_logs_df = self.__data_transformer.convert_to_integer(user_logs_df, [
            'num_25',
            'num_50',
            'num_75',
            'num_985',
            'num_100',
            'num_unq',

            # Transforming to int: this is a float number,
            # but its decimal points are not useful
            'total_secs'
        ])

        # Removing invalid total_secs
        user_logs_df = user_logs_df[user_logs_df['total_secs'] > 0]
        user_logs_df = user_logs_df[user_logs_df['total_secs'] < 2628000] # Less than one month

        user_logs_df['total_hours'] = user_logs_df['total_secs'] / 3600

        # Removing outliers
        user_logs_df = user_logs_df[user_logs_df['total_hours'] < 182]
        user_logs_df = user_logs_df[user_logs_df['num_25'] < 7458]
        user_logs_df = user_logs_df[user_logs_df['num_50'] < 395]
        user_logs_df = user_logs_df[user_logs_df['num_75'] < 215]
        user_logs_df = user_logs_df[user_logs_df['num_985'] < 2513]
        user_logs_df = user_logs_df[user_logs_df['num_100'] < 8414]
        user_logs_df = user_logs_df[user_logs_df['num_unq'] < 4362]

        # user_logs_df.drop('total_secs', inplace=True, axis=1)

        return user_logs_df
