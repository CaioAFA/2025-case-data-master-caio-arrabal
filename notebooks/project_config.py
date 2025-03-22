PARQUET_TRANSACTIONS_FILE_NAME = '../data/transactions.parquet'
PARQUET_MEMBERS_FILE_NAME = '../data/members.parquet'
PARQUET_USER_LOGS_FILE_NAME = '../data/user_logs.parquet'

DUCKDB_FILE_PATH = '../data/database.duckdb'
DUCKDB_TRANSACTIONS_DATABASE = 'transactions'
DUCKDB_MEMBERS_DATABASE = 'members'
DUCKDB_USER_LOGS_DATABASE = 'user_logs'

# Max safra = 201702, so we need to consider three months before
MAX_SAFRA_TO_CONSIDER_ON_DATA_PREPARE = 201611

COLUMNS_TO_GET_HISTORICAL_DATA = ['num_unq', 'total_secs', 'num_25', 'num_50', 'num_75', 'num_985', 'num_100']
SAFRAS_TO_CONSIDER_ON_HISTORICAL_DATA = [
    -2, # previous safras
    -1,
]

# Last processed dataset to test after fixing churn logic (22/03/2025)
PRE_PREPARED_DATA_TABLE = 'treated_churn_dataset_2025_03_21_19h36m'

# PRE_PREPARED_TABLE_DATA = 'treated_churn_dataset_2025_03_13_15h03m' # Best Dataset - 14/03/2025
# PRE_PREPARED_TABLE_DATA = 'treated_churn_dataset_2025_03_15_08h55m' # Last processed dataset after changes on 14/03/2025
# PRE_PREPARED_TABLE_DATA = 'treated_churn_dataset_2025_03_16_12h04m' # Last processed dataset to test after churn logic change (16/03/2025)
