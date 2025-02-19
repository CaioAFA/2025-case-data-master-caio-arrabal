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