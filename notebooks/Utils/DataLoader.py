import pandas as pd
from typing import Iterator, Callable
from Utils.DataTransformer import DataTransformer
import pyarrow.parquet as pq
import project_config


class DataLoader(object):
    def __init__(self):
        self.__data_transformer = DataTransformer()


    def __load_dataframe_in_chunks(
        self, file_name: str, chunksize: int, process_dataframe_method: Callable[[pd.DataFrame], pd.DataFrame]
    ):
        parquet_file = pq.ParquetFile(file_name)
        for batch in parquet_file.iter_batches(batch_size=chunksize):
            chunk = batch.to_pandas()
            chunk = process_dataframe_method(chunk)
            yield chunk


    def load_transactions_df(self) -> pd.DataFrame:
        transactions_df = pd.read_parquet(project_config.PARQUET_TRANSACTIONS_FILE_NAME)
        transactions_df = self.__data_transformer.process_transactions_df(transactions_df)
        return transactions_df


    def load_transactions_df_in_chunks(self, chunksize: int) -> Iterator[pd.DataFrame]:
        return self.__load_dataframe_in_chunks(
            project_config.PARQUET_TRANSACTIONS_FILE_NAME,
            chunksize=chunksize,
            process_dataframe_method=self.__data_transformer.process_transactions_df
        )


    def load_members_df(self) -> pd.DataFrame:
        members_df = pd.read_parquet(project_config.PARQUET_MEMBERS_FILE_NAME)
        members_df = self.__data_transformer.process_members_df(members_df)
        return members_df
    

    def load_members_df_in_chunks(self, chunksize: int) -> Iterator[pd.DataFrame]:
        return self.__load_dataframe_in_chunks(
            project_config.PARQUET_MEMBERS_FILE_NAME,
            chunksize=chunksize,
            process_dataframe_method=self.__data_transformer.process_members_df
        )
    

    def load_user_logs_df(self) -> pd.DataFrame:
        user_logs_df = pd.read_parquet(project_config.PARQUET_USER_LOGS_FILE_NAME)
        user_logs_df = self.__data_transformer.process_user_logs_df(user_logs_df)
        return user_logs_df
    

    def load_user_logs_df_in_chunks(self, chunksize: int) -> Iterator[pd.DataFrame]:
        return self.__load_dataframe_in_chunks(
            project_config.PARQUET_USER_LOGS_FILE_NAME,
            chunksize=chunksize,
            process_dataframe_method=self.__data_transformer.process_user_logs_df
        )