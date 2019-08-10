import os
from datetime import datetime

import config
from utils import date_utils
import quandl

def download_supplemental(start_date: datetime, end_date: datetime):
  start_date_str = date_utils.get_standard_ymd_format(start_date)
  end_date_str = date_utils.get_standard_ymd_format(end_date)

  destination_path = os.path.join(config.constants.SHAR_EQUITY_PRICES_DIR, f"sep_{start_date_str}_{end_date_str}.csv")

  download_sep(start_date, end_date, destination_path)

  return destination_path

def download_sep(start_date: datetime, end_date: datetime, destination_path: str):
  start_date_str = date_utils.get_standard_ymd_format(start_date)
  end_date_str = date_utils.get_standard_ymd_format(end_date)

  quandl.ApiConfig.api_key = config.constants.QUANDL_KEY
  df = quandl.get_table('SHARADAR/SEP', date={'gte': start_date_str, 'lte': end_date_str}, paginate=True)

  df.to_csv(destination_path)