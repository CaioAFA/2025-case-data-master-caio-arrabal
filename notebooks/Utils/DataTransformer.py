from typing import List
import pandas as pd


class DataTransformer(object):
    def convert_to_datetime(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = pd.to_datetime(df[dtf])

        return df
    

    def convert_to_integer(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = df[dtf].astype(int)

        return df


    def convert_to_boolean(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = df[dtf].astype(bool)

        return df


    def convert_to_category(self, df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        for dtf in fields:
            df[dtf] = df[dtf].astype('category')

        return df
    

    def split_datetime_field_in_year_month_and_day(
        self, df: pd.DataFrame, fields: List[str], del_fields: bool = True
    ) -> pd.DataFrame:
        '''
        Split datetime field into year, month, day, day of week and day of year
        '''
        for f in fields:
            year_field_name = f'{f}_year'
            month_field_name = f'{f}_month'
            day_field_name = f'{f}_day'
            day_of_week_field_name = f'{f}_day_of_week'
            day_of_year_field_name = f'{f}_day_of_year'

            df[year_field_name] = df[f].dt.year
            df[month_field_name] = df[f].dt.month
            df[day_field_name] = df[f].dt.day
            df[day_of_week_field_name] = df[f].dt.dayofweek
            df[day_of_year_field_name] = df[f].dt.dayofyear

            if del_fields:
                df.drop(f, inplace=True, axis=1)

        return df
    

    def create_cost_column(self, num_unq_col, total_secs_col) -> pd.DataFrame:
        result = 50 + (0.0051 * num_unq_col) + (0.0001 * total_secs_col)
        return result
    

    def create_net_profit_column(self, user_logs_df: pd.DataFrame) -> pd.DataFrame:
        def calc(row: pd.Series) -> float:
            return row['actual_amount_paid'] - row['cost']

        user_logs_df['net_profit'] = user_logs_df.apply(calc, axis=1)
        return user_logs_df
    

    def process_transactions_df(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_fields = [
            'transaction_date',
            'membership_expire_date'
        ]
        df = self.convert_to_datetime(df, dt_fields)
        df = self.split_datetime_field_in_year_month_and_day(df, dt_fields)

        df = self.convert_to_integer(df, [
            'payment_method_id',
            'payment_plan_days',
            'plan_list_price',
            'actual_amount_paid',

            # Transform into int, to transform into bool
            'is_auto_renew',
            'is_cancel'
        ])

        df = self.convert_to_boolean(df, [
            'is_auto_renew',
            'is_cancel'
        ])

        df['discount'] = df['plan_list_price'] - df['actual_amount_paid']

        # Remove negative values
        df['discount'] = df['discount'].apply(lambda x: x if x >= 0 else 0)

        df['price_per_month'] = df['actual_amount_paid'] / (df['payment_plan_days'] / 30)

        # Outliers
        df = df[df['membership_expire_date_year'] >= 2015]

        return df
    

    def process_members_df(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_fields = ['registration_init_time']
        df = self.convert_to_datetime(df, dt_fields)
        df = self.split_datetime_field_in_year_month_and_day(df, dt_fields)

        df = self.convert_to_category(df, [
            'city',
            'registered_via'
        ])

        df = self.convert_to_boolean(df, [
            'is_ativo'
        ])

        df = self.convert_to_integer(df, [
            'safra'
        ])

        df.rename({
            'is_ativo': 'is_active'
        }, inplace=True, axis=1)

        df.drop('gender', axis=1, inplace=True)
        df.drop('bd', axis=1, inplace=True)

        return df
    

    def process_user_logs_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.convert_to_integer(df, [
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
        df = df[df['total_secs'] > 0]
        df = df[df['total_secs'] < 2628000] # Less than one month

        df['total_hours'] = df['total_secs'] / 3600

        # Removing outliers
        df = df[df['total_hours'] < 182]
        df = df[df['num_25'] < 7458]
        df = df[df['num_50'] < 395]
        df = df[df['num_75'] < 215]
        df = df[df['num_985'] < 2513]
        df = df[df['num_100'] < 8414]
        df = df[df['num_unq'] < 4362]

        # user_logs_df.drop('total_secs', inplace=True, axis=1)

        return df
