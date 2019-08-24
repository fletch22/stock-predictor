import os
import random
import string
from pathlib import Path
from datetime import datetime

from utils import date_utils

APP_NAME = "stock_predictor"

def make_dir(dir):
  os.makedirs(dir, exist_ok=True)

PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))

HOME_DIR = str(Path.home())

WORKSPACE_DIR = os.path.join(HOME_DIR, "workspaces")
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'output')
DATA_DIR = os.path.join(WORKSPACE_DIR, "data")

FINANCE_DATA_DIR = os.path.join(DATA_DIR, "financial")
APP_FIN_OUTPUT_DIR = os.path.join(FINANCE_DATA_DIR, "output", APP_NAME)
make_dir(APP_FIN_OUTPUT_DIR)

QUANDL_DIR = os.path.join(FINANCE_DATA_DIR, "quandl")
QUANDL_TBLS_DIR = os.path.join(QUANDL_DIR, "tables")
SHAR_EQUITY_PRICES_DIR = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices")
SHAR_DAILY = os.path.join(QUANDL_TBLS_DIR, "shar_daily.csv")
SHAR_EQUITY_PRICES = os.path.join(SHAR_EQUITY_PRICES_DIR, "shar_equity_prices.csv")
SHAR_EQUITY_PRICES_MERGED = os.path.join(SHAR_EQUITY_PRICES_DIR, "shar_equity_prices_merged.csv")
SHAR_EQUITY_PRICES_SHORT = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices_shortlist.csv")
SHAR_EQUITY_PRICES_MED = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices_medlist.csv")
SHAR_CORE_FUNDAMENTALS = os.path.join(QUANDL_TBLS_DIR, 'shar_core_fundamentals.csv')

SHAR_SPLIT_EQUITY_EOD_DIR = os.path.join(QUANDL_TBLS_DIR, "splits_eod")
make_dir(SHAR_SPLIT_EQUITY_EOD_DIR)

SHAR_SPLIT_FUNDAMENTALS_DIR = os.path.join(QUANDL_TBLS_DIR, "splits_fundamentals")
make_dir(SHAR_SPLIT_FUNDAMENTALS_DIR)

CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
make_dir(CACHE_DIR)

GRAPHED_DIR = os.path.join(CACHE_DIR, 'graphed')
make_dir(GRAPHED_DIR)

LOG_PARENT_DIR = os.path.join(HOME_DIR, "logs", APP_NAME)
make_dir(LOG_PARENT_DIR)

env_unique_log_path = "UNIQIFY_LOG_PATH"
if env_unique_log_path in os.environ:
  print("Make unique: " + os.environ[env_unique_log_path])
if env_unique_log_path in os.environ and os.environ["UNIQIFY_LOG_PATH"] == "true":
  LOGGING_DIR = os.path.join(LOG_PARENT_DIR, date_utils.format_file_system_friendly_date(datetime.now()))
else:
  LOGGING_DIR = os.path.join(LOG_PARENT_DIR)

print(f"About to make logging dir (if nec): {LOGGING_DIR}")
make_dir(LOGGING_DIR)
LOGGING_FILE_PATH = os.path.join(LOGGING_DIR, 'stock-predictor.log')

CREDENTIALS_ROOT = os.path.join(PROJECT_DIR, "credentials")
make_dir(CREDENTIALS_ROOT)

CREDENTIALS_PATH = os.path.join(CREDENTIALS_ROOT, "fletch22-ai.json")

# Google Cloud Storage
GCS_UPLOAD_BUCKET_NAME = "api_uploads"

BACKUP_ROOT_PATH = "I:/sp_backups"
BACKUP_VOLUME_LABEL = "Flesche"

quandl_key_path = os.path.join(CREDENTIALS_ROOT, "quandl_key.txt")
with open(quandl_key_path, "r") as f:
  QUANDL_KEY = f.read()

wtd_key_path = os.path.join(CREDENTIALS_ROOT, "world_trading_data_key.txt")
with open(wtd_key_path, "r") as f:
  WTD_KEY = f.read()
