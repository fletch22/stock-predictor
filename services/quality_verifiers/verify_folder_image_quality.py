import os
import random
from datetime import timedelta

import config
from config import logger_factory
from services import file_services
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

def verify():
  sought_gain_frac = .01
  # package_folder = "tsczuz__2019-08-15_23-46-28-904.99"  # 2019-08-13 .56 -> -.52
  # package_folder = "tsczuz__2019-08-15_21-57-05-587.24"  # 2019-08-12 .56 -> -.98
  package_folder = "tsczuz__2019-08-13_08-34-11-93.47"

  data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "test_one_day")
  image_dir = os.path.join(data_cache_dir, package_folder)

  files = file_services.walk(image_dir)
  random.shuffle(files)

  logger.info(f"Number of files found: {len(files)}")

  files = files[:20]
  for f in files:
    basename = os.path.basename(f)
    cat_actual, symbol, date_str = get_info_from_file_path(f)

    logger.info(f"{symbol}; date: {date_str}")

    yield_date = date_utils.parse_std_datestring(date_str)
    bet_date = date_utils.parse_std_datestring(date_str) + timedelta(days=-1)

    price_data = RealtimeEquityPriceService.get_historical_price(symbol, start_date=bet_date, end_date=yield_date)

    print(price_data)

    bet_date_str = date_utils.get_standard_ymd_format(bet_date)

    if 'history' in price_data.keys():
      logger.info("Found history!")
      history = price_data['history']

      if bet_date_str in history.keys():
        bet_date_data = history[bet_date_str]

        bet_price = float(bet_date_data['close'])

        if date_str in history.keys():
          yield_date_data = history[date_str]

          # close = float(yield_date_data['close'])
          high = float(yield_date_data['high'])

          gain = (high - bet_price) / bet_price
          cat_actual_rt = "0" if gain < sought_gain_frac else "1"

          print(f"{basename}; bp: {bet_price}; high: {high}; g: {gain}; fcat: {cat_actual}; scat: {cat_actual_rt}")

def get_info_from_file_path(file_path: str):
  basename = os.path.basename(file_path)
  parts = basename.split("_")
  cat_actual = parts[0]
  symbol = parts[1]  # .replace("-", "_")
  date_str = parts[2].split('.')[0]

  return cat_actual, symbol, date_str