import pandas as pd
from typing import List


class DataLoader(object):
    def load_transactions_df(self) -> pd.DataFrame:
        transactions_df = pd.read_parquet('../data/transactions.parquet')

        transactions_df = self.__convert_to_datetime(transactions_df, [
            'transaction_date',
            'membership_expire_date'
        ])

        transactions_df = self.__convert_to_integer(transactions_df, [
            'payment_method_id',
            'payment_plan_days',
            'plan_list_price',
            'actual_amount_paid',

            # Transform into int, to transform into bool
            'is_auto_renew',
            'is_cancel'
        ])

        transactions_df = self.__convert_to_boolean(transactions_df, [
            'is_auto_renew',
            'is_cancel'
        ])

        # Unused field
        transactions_df.drop('safra', axis=1, inplace=True)

        return transactions_df
    

    def load_members_df(self) -> pd.DataFrame:
        members_df = pd.read_parquet(
            '../data/members.parquet'
        )

        members_df = self.__convert_to_datetime(members_df, [
            'registration_init_time'
        ])

        members_df = self.__convert_to_category(members_df, [
            'city',
            'gender',
            'registered_via'
        ])

        members_df = self.__convert_to_integer(members_df, [
            'bd',
        ])

        members_df = self.__convert_to_boolean(members_df, [
            'is_ativo'
        ])

        return members_df


    def __convert_to_datetime(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = pd.to_datetime(df[dtf])

        return df
    

    def __convert_to_integer(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = df[dtf].astype(int)

        return df


    def __convert_to_boolean(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = df[dtf].astype(bool)

        return df


    def __convert_to_category(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = df[dtf].astype('category')

        return df
