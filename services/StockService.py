import os
from datetime import datetime

from pandas import DataFrame

import config


class StockService:

  def get_stock(self, symbol, start_date: datetime, end_date: datetime):
    # get from cache if exists
    # get hash filename of date
    filename = self.get_cache_filename(symbol, start_date, end_date)

    cache_file_path = os.path.join(config.constants.CACHE_DIR, filename)

    # check if hash filename exists
    does_exists = os.path.exists(cache_file_path)

    df = None
    if does_exists:
      # return to caller as DF
      df = DataFrame.from_csv(cache_file_path)
    else:
      # hash does not exist.
      # Get from SHAR_DAILY.csv
      shar_daily = DataFrame.from_csv(config.constants.SHAR_DAILY)

      # Use filter on DF
      # Save df as file in cache folder

    return df

  def get_cache_filename(self, symbol, start_date: datetime, end_date: datetime):
    symbol_final = symbol.lower()

    return f'{symbol_final}-{start_date.strftime("%Y-%d")}-{start_date.strftime("%Y-%d")}.csv'
