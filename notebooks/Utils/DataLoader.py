import pandas as pd
from typing import List
from Utils.DataTransformer import DataTransformer


class DataLoader(object):
    def __init__(self):
        self.__data_transformer = DataTransformer()


    def load_transactions_df(self) -> pd.DataFrame:
        transactions_df = pd.read_parquet('../data/transactions.parquet')

        dt_fields = [
            'transaction_date',
            'membership_expire_date'
        ]
        transactions_df = self.__data_transformer.convert_to_datetime(transactions_df, dt_fields)
        transactions_df = self.__data_transformer.split_datetime_field_in_year_month_and_day(transactions_df, dt_fields)

        transactions_df = self.__data_transformer.convert_to_integer(transactions_df, [
            'payment_method_id',
            'payment_plan_days',
            'plan_list_price',
            'actual_amount_paid',

            # Transform into int, to transform into bool
            'is_auto_renew',
            'is_cancel'
        ])

        transactions_df = self.__data_transformer.convert_to_boolean(transactions_df, [
            'is_auto_renew',
            'is_cancel'
        ])

        transactions_df['discount'] = transactions_df['plan_list_price'] - transactions_df['actual_amount_paid']

        # Remove negative values
        transactions_df['discount'] = transactions_df['discount'].apply(lambda x: x if x >= 0 else 0)

        transactions_df['price_per_month'] = transactions_df['actual_amount_paid'] / (transactions_df['payment_plan_days'] / 30)

        # Unused field
        transactions_df.drop('safra', axis=1, inplace=True)

        # Outliers
        transactions_df = transactions_df[transactions_df.membership_expire_date.dt.year >= 2015]

        return transactions_df
    

    def load_members_df(self, nrows: int = None) -> pd.DataFrame:
        members_df = pd.read_parquet(
            '../data/members.parquet'
        )

        if nrows is not None:
            members_df = members_df[:nrows]

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

        user_logs_df.drop('total_secs', inplace=True, axis=1)

        return user_logs_df

