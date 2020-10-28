import json
import os
import random
import string
from pathlib import Path
from datetime import datetime

# from utils import date_utils

APP_NAME = "stock_predictor"

def make_dir(dir):
  os.makedirs(dir, exist_ok=True)

PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))

HOME_DIR = str(Path.home())

OUTPUT_DIR = os.path.join(PROJECT_DIR, 'output')
DATA_DIR = os.path.join("d:\\workspaces", "data")

FINANCE_DATA_DIR = os.path.join(DATA_DIR, "financial")
APP_FIN_OUTPUT_DIR = os.path.join(FINANCE_DATA_DIR, "output", APP_NAME)
make_dir(APP_FIN_OUTPUT_DIR)

ALT_MODEL_OUTPUT_DIR = os.path.join(APP_FIN_OUTPUT_DIR, "alt_models")
make_dir(ALT_MODEL_OUTPUT_DIR)

QUANDL_DIR = os.path.join(FINANCE_DATA_DIR, "quandl")
QUANDL_TBLS_DIR = os.path.join(QUANDL_DIR, "tables")

SHAR_EQUITY_PRICES_DIR = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices")
SHAR_DAILY = os.path.join(QUANDL_TBLS_DIR, "shar_daily.csv")
SHAR_EQUITY_PRICES = os.path.join(SHAR_EQUITY_PRICES_DIR, "shar_equity_prices_2020-10-17.csv")
SHAR_EQUITY_PRICES_MERGED = os.path.join(SHAR_EQUITY_PRICES_DIR, "shar_equity_prices_merged.csv")
SHAR_EQUITY_PRICES_SHORT = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices_shortlist.csv")
SHAR_EQUITY_PRICES_MED = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices_medlist.csv")
SHAR_CORE_FUNDAMENTALS = os.path.join(QUANDL_TBLS_DIR, 'shar_core_fundamentals.csv')
SHAR_CORE_FUNDAMENTALS_ARCHIVE = Path(QUANDL_TBLS_DIR, 'shar_core_fundamentals_archive')
SHAR_TICKERS = os.path.join(QUANDL_TBLS_DIR, "shar_tickers.csv")

TEST_DATA_FILES = os.path.join(FINANCE_DATA_DIR, "test_files")

WORLD_TRADING_DIR = os.path.join(FINANCE_DATA_DIR, "world_trading_data", "tables")
WTD_STOCK_LIST_PATH = os.path.join(WORLD_TRADING_DIR, "worldtradingdata-stocklist.csv")

SHAR_SPLIT_EQUITY_EOD_DIR = os.path.join(QUANDL_TBLS_DIR, "splits_eod")
make_dir(SHAR_SPLIT_EQUITY_EOD_DIR)

SHAR_SPLIT_FUNDAMENTALS_DIR = os.path.join(QUANDL_TBLS_DIR, "splits_fundamentals")
make_dir(SHAR_SPLIT_FUNDAMENTALS_DIR)

SHARADAR_ACTIONS_DIR = os.path.join(QUANDL_TBLS_DIR, "shar_actions")
make_dir(SHARADAR_ACTIONS_DIR)

SHARADAR_ACTIONS_FILEPATH = Path(SHARADAR_ACTIONS_DIR, "actions.csv")

CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
make_dir(CACHE_DIR)

GRAPHED_DIR = os.path.join(CACHE_DIR, 'graphed')
make_dir(GRAPHED_DIR)

LOG_PARENT_DIR = os.path.join(HOME_DIR, "logs", APP_NAME)
make_dir(LOG_PARENT_DIR)

env_unique_log_path = "UNIQIFY_LOG_PATH"
if env_unique_log_path in os.environ:
  print("Make unique: " + os.environ[env_unique_log_path])
LOGGING_DIR = os.path.join(LOG_PARENT_DIR)

print(f"About to make logging dir (if nec): {LOGGING_DIR}")
make_dir(LOGGING_DIR)
LOGGING_FILE_PATH = os.path.join(LOGGING_DIR, 'stock-predictor.log')

LOG_PREFIX="custom_"
LOGGING_DIR_SPARK=None

CREDENTIALS_ROOT = os.path.join("C:\\Users\\Chris\\workspaces\\data\\", "credentials")
make_dir(CREDENTIALS_ROOT)

CREDENTIALS_PATH = os.path.join(CREDENTIALS_ROOT, "fletch22-ai.json")

# Google Cloud Storage
GCS_UPLOAD_BUCKET_NAME = "fletch22-ai-vcm"

BACKUP_ROOT_PATH = "I:/sp_backups"
BACKUP_VOLUME_LABEL = "Flesche"

quandl_key_path = os.path.join(CREDENTIALS_ROOT, "quandl_key.txt")

print(f'qkp: {quandl_key_path}')

with open(quandl_key_path, "r") as f:
  QUANDL_KEY = f.read()

wtd_key_path = os.path.join(CREDENTIALS_ROOT, "world_trading_data_key.txt")
with open(wtd_key_path, "r") as f:
  WTD_KEY = f.read()

TEST_PACKAGE_FOLDER = os.path.join(CACHE_DIR, "test_package_1")
make_dir(TEST_PACKAGE_FOLDER)

IS_IN_SPARK_CONTEXT = False
SPARK_LOGGING_PATH = None

alpaca_cred_path = os.path.join(CREDENTIALS_ROOT, 'alpaca.json')
with open(alpaca_cred_path, "r") as f:
  ALPACA_CREDENTIALS = json.loads(f.read())

slack_cred_path = os.path.join(CREDENTIALS_ROOT, 'slack.json')
with open(slack_cred_path, "r") as f:
  SLACK_CREDENTIALS = json.loads(f.read())

BET_HISTORY_DIR = os.path.join(APP_FIN_OUTPUT_DIR, APP_NAME, "bet")
make_dir(BET_HISTORY_DIR)
BET_HIST_CSV_PATH = os.path.join(BET_HISTORY_DIR, "bet_history.csv")


ALPHA_MEDIA_SIGNAL_PROJ = Path(Path(PROJECT_DIR).parent, "alpha_media_signal")





