import os
import random
import string

from datetime import datetime

from utils import date_utils


def make_dir(dir):
  os.makedirs(dir, exist_ok=True)

PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))
WORKSPACE_DIR = os.path.dirname(PROJECT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'output')
DATA_DIR = os.path.join(WORKSPACE_DIR, "data")

FINANCE_DATA_DIR = os.path.join(DATA_DIR, "financial")
APP_FIN_OUTPUT_DIR = os.path.join(FINANCE_DATA_DIR, "output", "stock_predictor")
make_dir(APP_FIN_OUTPUT_DIR)

QUANDL_DIR = os.path.join(FINANCE_DATA_DIR, "quandl")
QUANDL_TBLS_DIR = os.path.join(QUANDL_DIR, "tables")
SHAR_DAILY = os.path.join(QUANDL_TBLS_DIR, "shar_daily.csv")
SHAR_EQUITY_PRICES = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices.csv")
SHAR_EQUITY_PRICES_SHORT = os.path.join(QUANDL_TBLS_DIR, "shar_equity_prices_shortlist.csv")

SHAR_SPLIT_EQUITY_PRICES_DIR = os.path.join(QUANDL_TBLS_DIR, "splits")
make_dir(SHAR_SPLIT_EQUITY_PRICES_DIR)

CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
make_dir(CACHE_DIR)

GRAPHED_DIR = os.path.join(CACHE_DIR, 'graphed')
make_dir(GRAPHED_DIR)

env_unique_log_path = "UNIQIFY_LOG_PATH"
if env_unique_log_path in os.environ:
  print("Make unique: " + os.environ[env_unique_log_path])
if env_unique_log_path in os.environ and os.environ["UNIQIFY_LOG_PATH"] == "true":
  LOGGING_DIR = os.path.join(OUTPUT_DIR, 'logs', date_utils.format_file_system_friendly_date(datetime.now()))
else:
  LOGGING_DIR = os.path.join(OUTPUT_DIR, 'logs')

print(f"About to make logging dir (if nec): {LOGGING_DIR}")
make_dir(LOGGING_DIR)
LOGGING_FILE_PATH = os.path.join(LOGGING_DIR, 'stock-predictor.log')

CREDENTIALS_ROOT = os.path.join(PROJECT_DIR, "credentials")
make_dir(CREDENTIALS_ROOT)

CREDENTIALS_PATH = os.path.join(CREDENTIALS_ROOT, "fletch22-ai.json")

# Google Cloud Storage
GCS_UPLOAD_BUCKET_NAME = "api_uploads"
