PARQUET_TRANSACTIONS_FILE_NAME = '../data/transactions.parquet'
PARQUET_MEMBERS_FILE_NAME = '../data/members.parquet'
PARQUET_USER_LOGS_FILE_NAME = '../data/user_logs.parquet'

DUCKDB_FILE_PATH = '../data/database.duckdb'
DUCKDB_TRANSACTIONS_DATABASE = 'transactions'
DUCKDB_MEMBERS_DATABASE = 'members'
DUCKDB_USER_LOGS_DATABASE = 'user_logs'

# Last processed dataset to test after fixing churn logic (22/03/2025)
PRE_PREPARED_DATA_TABLE = 'treated_churn_dataset_2025_03_21_19h36m'

# Table with prediction data
PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_03_20h18m'

# Max safra = 201702, so we need to consider three months before
MAX_SAFRA_TO_CONSIDER_ON_DATA_PREPARE = 201611

COLUMNS_TO_GET_HISTORICAL_DATA = ['num_unq', 'total_secs', 'num_25', 'num_50', 'num_75', 'num_985', 'num_100']
SAFRAS_TO_CONSIDER_ON_HISTORICAL_DATA = [
    -2, # previous safras
    -1,
]


"avg(transaction_date_month)"	"avg(safra_month)"	"avg(membership_expire_date_month)"

COLUMNS_USED_ON_TRAIN_AND_PREDICTION = [
    'registration_init_time_day', 'registration_init_time_year',

    # Removed to solve churn errors
    #'transaction_date_day_of_year',
    #'transaction_date_month',
    # 'membership_expire_date_day_of_year',
    # 'membership_expire_date_month',
    # 'safra_month',
    # 'transaction_date_day',
    # 'transaction_date_day_of_week',
    # 'registration_init_time_day_of_year'
    # 'membership_expire_date_day',
    # 'membership_expire_date_year',


    'is_auto_renew', 'is_cancel', 'remaining_days',
    

    # Historical Data
    'num_100', 'num_100-1M', 'num_100-2M',
    'num_25', 'num_25-1M', 'num_25-2M',
    'num_50', 'num_50-1M', 'num_50-2M',
    'num_75', 'num_75-1M', 'num_75-2M',
    'num_985', 'num_985-1M', 'num_985-2M',
    'num_unq', 'num_unq-1M', 'num_unq-2M',
    'total_secs', 'total_secs-1M', 'total_secs-2M',

    # Categorical Data
    'city_0', 'city_1', 'city_10', 'city_11', 'city_12', 'city_13', 'city_14', 'city_15', 'city_16', 'city_17', 'city_18', 'city_19', 'city_2', 'city_20', 'city_21', 'city_22', 'city_3', 'city_4', 'city_5', 'city_6', 'city_7', 'city_8', 'city_9',

    'registered_via_0', 'registered_via_1', 'registered_via_10', 'registered_via_11', 'registered_via_12', 'registered_via_13', 'registered_via_14', 'registered_via_15', 'registered_via_16', 'registered_via_17', 'registered_via_18', 'registered_via_19', 'registered_via_2', 'registered_via_3', 'registered_via_4', 'registered_via_5', 'registered_via_6', 'registered_via_7', 'registered_via_8', 'registered_via_9',

    'payment_method_id_0', 'payment_method_id_1', 'payment_method_id_10', 'payment_method_id_11', 'payment_method_id_12', 'payment_method_id_13', 'payment_method_id_14', 'payment_method_id_15', 'payment_method_id_16', 'payment_method_id_17', 'payment_method_id_18', 'payment_method_id_19', 'payment_method_id_2', 'payment_method_id_20', 'payment_method_id_21', 'payment_method_id_22', 'payment_method_id_23', 'payment_method_id_24', 'payment_method_id_25', 'payment_method_id_26', 'payment_method_id_27', 'payment_method_id_28', 'payment_method_id_29', 'payment_method_id_3', 'payment_method_id_30', 'payment_method_id_31', 'payment_method_id_32', 'payment_method_id_33', 'payment_method_id_34', 'payment_method_id_35', 'payment_method_id_36', 'payment_method_id_37', 'payment_method_id_38', 'payment_method_id_39', 'payment_method_id_4', 'payment_method_id_40', 'payment_method_id_41', 'payment_method_id_5', 'payment_method_id_6', 'payment_method_id_7', 'payment_method_id_8', 'payment_method_id_9',
]

TARGET_COLUMN = 'is_churn'

# Model after splitting data into train / validation by safra datetime, not with
# train_test_split randomly
SELECTED_MODEL = './models/random_forest_model_2025-04-02-08h-56m.joblib'

TRAIN_DATA_UNTIL_SAFRA = 201608
TEST_DATA_UNTIL_SAFRA = 201611

# TESTING
SELECTED_MODEL = './models/random_forest_model_2025-04-04-21h-36m.joblib' # Testing new model
PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_04_21h40m' # Testing

SELECTED_MODEL = './models/random_forest_model_2025-04-04-22h-04m.joblib' # Testing new model
PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_04_22h05m' # Testing

SELECTED_MODEL = './models/random_forest_model_2025-04-04-22h-45m.joblib' # Testing new model
PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_04_22h46m' # Testing

SELECTED_MODEL = './models/random_forest_model_2025-04-04-23h-08m.joblib' # Testing: without balancing true / false dfs
PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_04_23h09m' # Testing

# SELECTED_MODEL = './models/random_forest_model_2025-04-06-09h-59m.joblib' # Testing: more false than true (// 0.6)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_06_10h00m' # Testing

# SELECTED_MODEL = './models/random_forest_model_2025-04-07-20h-46m.joblib' # Testing: more false than true (// 0.83)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_07_20h47m' # Testing

SELECTED_MODEL = './models/random_forest_model_2025-04-05-08h-05m.joblib' # Testing: more false than true (// 0.7)
PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_06_09h36m' # Testing

# # Testing
# SELECTED_MODEL = './models/random_forest_model_2025-04-07-19h-25m.joblib'
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_07_19h28m' # Testing


# SELECTED_MODEL = './models/random_forest_model_2025-04-06-10h-29m.joblib' # Testing: more false than true (// 0.78)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_06_10h29m' # Testing

# SELECTED_MODEL = './models/random_forest_model_2025-04-06-10h-10m.joblib' # Testing: more false than true (// 0.83)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_06_10h11m' # Testing

# SELECTED_MODEL = './models/random_forest_model_2025-04-05-08h-37m.joblib' # Testing: more false than true (// 0.5)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_05_08h37m' # Testing

########### TRAINING AFTER REMOVING SOME FIELDS FROM PROJECT
# SELECTED_MODEL = './models/random_forest_model_2025-04-07-21h-31m.joblib' # Testing: more false than true (// 0.9)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_07_21h31m' # Testing

# SELECTED_MODEL = './models/random_forest_model_2025-04-07-21h-40m.joblib' # Testing: more false than true (// 0.7)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_07_21h41m' # Testing

# SELECTED_MODEL = './models/random_forest_model_2025-04-07-21h-51m.joblib' # Testing: more false than true (// 0.6)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_07_21h52m' # Testing


# ########### TRAINING AFTER REMOVING MORE FIELDS FROM PROJECT
# SELECTED_MODEL = './models/random_forest_model_2025-04-07-22h-16m.joblib' # Testing: more false than true (// 0.7)
# PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_07_22h16m' # Testing

# BEST MODEL!
# Test with this parameters
SELECTED_MODEL = './models/random_forest_model_2025-04-08-18h-37m.joblib' # Testing: more false than true (// 0.6)
PREDICTED_DATA_TABLE = 'predicted_dataset_2025_04_08_18h36m' # Testing