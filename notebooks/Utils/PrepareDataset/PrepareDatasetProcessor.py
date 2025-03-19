import pandas as pd
import project_config
from Utils.DuckDb.DuckDb import DuckDb
from Utils.Safra import SafraUtils
from typing import List
from datetime import datetime
import math


class PrepareDatasetProcessor(object):
    def __init__(
        self,
        duck_db: DuckDb,
        duck_db_table_name: str,
        process_users_batch_size: int,
        max_safra_to_consider: int
    ):
        self.__duck_db = duck_db
        self.__duck_db_table_name = duck_db_table_name
        self.__process_users_batch_size = process_users_batch_size
        self.__max_safra_to_consider = max_safra_to_consider

        self.__safra_utils = SafraUtils()


    def process_dataframe(
        self,
        full_dataframe: pd.DataFrame,
        users_msno: List[str]
    ):
        # Process in batches
        total_iter = math.floor(len(full_dataframe) / self.__process_users_batch_size)

        print(f'Total de iterações: {total_iter}')
        print(f'Tamanho do Dataframe completo: {len(full_dataframe)}')

        result = None
        for i in range(0, total_iter):
            start = self.__process_users_batch_size * i
            end = self.__process_users_batch_size * (i + 1)

            print(f'{datetime.now()} Processando usuários ({start} / {end})')

            batch_users = users_msno[start:end]

            print(f'{datetime.now()} Separando um DF apenas com os usuários em questão')
            users_df = full_dataframe[full_dataframe['msno'].isin(batch_users)]

            print(f'Tamanho do Dataframe dos usuários do batch: {len(users_df)}')

            treated_df = self.__treat_df(
                users_df,
                batch_users
            )

            print(f'Tamanho da Dataframe: {len(treated_df)}')
            if len(treated_df) == 0:
                continue

            print('Removendo linhas sem informações de membros suficientes')
            treated_df = treated_df[~treated_df['members_msno'].isna()]

            # print('Removendo linhas sem informações suficientes para calcular o churn')
            # treated_df = treated_df[treated_df['no_churn_information'] == False]

            print(f'Tamanho da Dataframe pós filtros: {len(treated_df)}')

            self.__duck_db.create_database_table(treated_df)
            if i == 0:
                print('Truncando tabela')
                self.__duck_db.truncate_table(self.__duck_db_table_name)

            self.__duck_db.upload_dataframe_to_duck_db(treated_df, self.__duck_db_table_name)

            print('#' * 50)

            result = treated_df

        # Return the last processed one for debug purposes
        return result
    

    def __treat_df(self, df: pd.DataFrame, users_msno: List[str]) -> pd.DataFrame:
        global USER_ROW_TO_COPY_MEMBER_INFO

        df_by_users = {}

        print(f'Separando DataFrames por usuários')
        for index, usr in enumerate(users_msno):
            if index % 1000 == 0:
                print(f'-> {index} / {len(users_msno)}')
                
            df_by_users[usr] = df[df['msno'] == usr]

        rows = []
        users_qty = len(df_by_users.values())
        count = 0
        for msno, user_df in df_by_users.items():
            # print(f'Processando usuário {msno}')

            count += 1
            if count % 500 == 0:
                print(f'{datetime.now()} Processando usuário {count}/{users_qty} ({msno})')

            for _, user_row in user_df.iterrows():
                user_row = self.__fill_out_members_data_if_needed(user_df, user_row)
                # user_row = self.__calc_row_churn(user_df, user_row)
                user_row = self.__calc_row_churn_v2(user_df, user_row)

                user_row = self.__get_previous_months_cols_values(
                    user_df,
                    user_row,
                    ['num_unq', 'total_secs', 'num_25', 'num_50', 'num_75', 'num_985', 'num_100']
                )

                rows.append(user_row)
                # print(user_row)

            # Resetting this cache
            USER_ROW_TO_COPY_MEMBER_INFO = {}

        result = pd.DataFrame(rows)
        return result
    

    def __calc_row_churn_v2(self, user_df: pd.DataFrame, row: pd.Series):
        '''
        Consider only the third month ahead
        '''
        months_to_consider_churn = 3

        is_churn = False
        no_churn_information = False

        # print('#' * 100)
        # print(f'User: {row["msno"]}')
        # print(f'Current safra: {row["safra"]}')

        next_safra = self.__safra_utils.get_next_safras(row['safra'], months_to_consider_churn)
        # print(f'Next safra: {next_safra}')

        # Can't obtain info from this safra so on
        if row["safra"] > self.__max_safra_to_consider:
            # print('current safra > self.__max_safra_to_consider!')
            no_churn_information = True

        else:
            next_safra_row = user_df[user_df['safra'] == next_safra].reset_index()

            # No more payment info, consider churn
            if len(next_safra_row) == 0:
                # print(f'Safra {next_safra} não encontrada, pulando')
                is_churn = True

            # Canceled, is churn
            elif next_safra_row['is_cancel'][0] == True:
                # print(f'Safra {next_safra} encontrada com is_cancel, marcando como churn!')
                is_churn = True

        row['is_churn'] = is_churn
        row['no_churn_information'] = no_churn_information
        return row


    def __fill_out_members_data_if_needed(self, user_df: pd.DataFrame, row: pd.Series):
        # Sometimes, we don't have the member information from all safras,
        # so we'll copy the values from the closer month

        members_msno_col = 'members_msno'

        # Already filled out
        if not row[members_msno_col] == None:
            row['_filled_out_members_info'] = 'Infos já existentes'
            return row

        # No user info available in any safras
        safras_df = user_df[~user_df[members_msno_col].isna()]
        if len(safras_df) == 0:
            row['_filled_out_members_info'] = 'Sem infos'
            return row

        members_cols_to_consider = [
            'city', 'registered_via', 'is_active', 'registration_init_time_year',
            'registration_init_time_month', 'registration_init_time_day',
            'registration_init_time_day_of_week', 'registration_init_time_day_of_year',
            'members_msno'
        ]

        if row['msno'] not in USER_ROW_TO_COPY_MEMBER_INFO:
            current_safra = row['safra']

            min_safra = safras_df['safra'].min()
            max_safra = safras_df['safra'].max()

            safra_to_consider = min_safra if current_safra < max_safra else max_safra

            row_to_copy = safras_df[safras_df['safra'] == safra_to_consider]
            row_to_copy = row_to_copy.reset_index()
            USER_ROW_TO_COPY_MEMBER_INFO[row['msno']] = row_to_copy
        else:
            row_to_copy = USER_ROW_TO_COPY_MEMBER_INFO[row['msno']]

        row['_filled_out_members_info'] = 'Copiadas'
        for col in members_cols_to_consider:
            row[col] = row_to_copy[col][0]

        return row
    

    def __get_previous_months_cols_values(self, user_df: pd.DataFrame, row: pd.Series, cols: List[str]) -> pd.DataFrame:
        current_safra = row['safra']

        safras_to_consider = [
            -2, # previous safras
            -1,
        ]

        if not self.__all_safras_exist(user_df, current_safra, safras_to_consider):
            # print(f'Linha sem safras {safras_to_consider}')
            return row
        
        for col in cols:
            for safra_modifier in safras_to_consider:
                safra = self.__safra_utils.get_next_safras(current_safra, safra_modifier)
                safra_row = user_df[user_df['safra'] == safra].reset_index()
                title = f'{col}{safra_modifier if safra_modifier < 0 else f"+{safra_modifier}"}M'
                row[title] = safra_row[col][0]

        # print(f'Linha {row} ajustada')
        return row


    def __all_safras_exist(self, user_df: pd.DataFrame, current_safra: int, safras_to_consider: List[int]) -> bool:
        for safra_modifier in safras_to_consider:
            next_safra = self.__safra_utils.get_next_safras(current_safra, safra_modifier)
            filtered = user_df[user_df['safra'] == next_safra]

            if len(filtered) == 0:
                return False
            
        return True
