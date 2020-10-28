import os
from datetime import datetime
from pathlib import Path

import config
from config import constants
from utils import date_utils
import quandl

def download_sep(start_date: datetime, end_date: datetime, destination_path: str):
  start_date_str = date_utils.get_standard_ymd_format(start_date)
  end_date_str = date_utils.get_standard_ymd_format(end_date)

  quandl.ApiConfig.api_key = config.constants.QUANDL_KEY
  df = quandl.get_table('SHARADAR/SEP', date={'gte': start_date_str, 'lte': end_date_str}, paginate=True)

  df.to_csv(destination_path, index=False)

def download_actions():
  quandl.ApiConfig.api_key = config.constants.QUANDL_KEY
  df = quandl.get_table('SHARADAR/ACTIONS', date={'gte': "1995-01-01", 'lte': "2999-01-01"}, paginate=True)

  df.to_csv(constants.SHARADAR_ACTIONS_FILEPATH, index=False)
