import os
import random
import statistics
from datetime import datetime
from statistics import mean

from google.cloud import automl_v1beta1 as automl

import config
from config import logger_factory
from services import file_services
from services.Eod import Eod
from services.EquityUtilService import EquityUtilService
from services.StockService import StockService
from services.spark import spark_predict
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class AutoMlPredictionService():
  client = None
  package_dir = None
  short_model_id = None

  def __init__(self, short_model_id: str, package_dir: str, score_threshold: float = 0.5):
    self.client = automl.PredictionServiceClient.from_service_account_file(config.constants.CREDENTIALS_PATH)

    self.score_threshold = score_threshold
    self.package_dir = package_dir
    self.short_model_id = short_model_id

  def predict_and_calculate(self, task_dir: str, image_dir: str, sought_gain_frac: float, max_files=-1, purge_cached: bool = False):
    file_paths = file_services.walk(image_dir)
    random.shuffle(file_paths, random.random)

    if max_files > -1 and len(file_paths) > max_files:
      file_paths = file_paths[:max_files]

    # Act
    image_info = []
    for f in file_paths:
      category_actual, symbol, yield_date_str = EquityUtilService.get_info_from_file_path(f)
      yield_date = date_utils.parse_datestring(yield_date_str)

      start_date = date_utils.parse_datestring('2019-07-17')

      if yield_date > start_date:
        logger.info(f"{symbol} using yield date {yield_date_str}")
        image_info.append({"category_actual": category_actual,
                           "symbol": symbol,
                           "file_path": f,
                           "score_threshold": self.score_threshold,
                           "short_model_id": self.short_model_id,
                           "purged_cached": purge_cached,
                           })

    results = spark_predict.get_spark_results(image_info, task_dir)

    logger.info(f"Results: {len(results)}")

    unique_symbols = set()
    for r in results:
      unique_symbols.add(r["symbol"])
    logger.info(f"Unique symbols: {len(unique_symbols)}")

    results_filtered_1 = [d for d in results if d['category_predicted'] == "1"]
    logger.info(f"Found category_predicted = 1: {len(results_filtered_1)}.")

    count_yielded = 0
    logger.info("About to get shar equity data for comparison calcs.")
    aggregate_gain = []
    max_drop = 1.0
    invest_amount = 10000

    for r in results_filtered_1:
      eod_info = StockService.get_eod_of_date(r["symbol"], date_utils.parse_datestring(r["date"]))
      bet_price = eod_info["bet_price"]
      high = eod_info["high"]
      close = eod_info[Eod.CLOSE]
      low = eod_info["low"]
      score = r["score"]
      category_actual = r["category_actual"]
      category_predicted = r["category_predicted"]
      symbol = r["symbol"]
      yield_date_str = r["date"]
      open = eod_info['open']

      count_yielded, invest_amount, aggregate_gain = self.calc_frac_gain(aggregate_gain, bet_price, category_actual, category_predicted, open, high, low, close, count_yielded, invest_amount, max_drop, score,
                                                                         sought_gain_frac, symbol, date_utils.parse_datestring(yield_date_str))

    for n in aggregate_gain:
      logger.info(n)

    total_samples = len(aggregate_gain)
    if total_samples > 0:
      logger.info(f"Accuracy: {count_yielded / total_samples}; total: {total_samples}; mean frac: {mean(aggregate_gain)}; total: {invest_amount}")
    else:
      logger.info("No samples to calculate.")

  def calc_frac_gain(self, aggregate_gain, bet_price, category_actual: str, category_predicted: str, open: float, high: float, low:float, close: float, count: int, initial_investment_amount: float, max_drop: float, score: float, sought_gain_frac: float, symbol: str, yield_date: datetime):
    earned_amount = initial_investment_amount

    if bet_price is None:
      return count, earned_amount, aggregate_gain

    logger.info(f"{symbol} Yield Date: {date_utils.get_standard_ymd_format(yield_date)} Score: {score}; score_threshold: {self.score_threshold}")
    max_drop_price = bet_price - (max_drop * bet_price)
    logger.info(f"\tbet_price: {bet_price}; open: {open}; high: {high}; low: {low}; close: {close}; max_drop_price: {max_drop_price}; ")
    if score >= self.score_threshold:
      if open > bet_price:
        frac_return = (open - bet_price) / bet_price
        aggregate_gain.append(frac_return)
        count += 1
      elif category_actual == category_predicted:
        sought_gain_price = bet_price + (bet_price * sought_gain_frac)

        # if low < max_drop_price:
        #   frac_return = -1 * max_drop
        #   aggregate_gain.append(frac_return)
        # el
        if high > sought_gain_price:
          frac_return = sought_gain_frac
          aggregate_gain.append(frac_return)
        else:
          frac_return = (close - bet_price) / bet_price
          aggregate_gain.append(frac_return)

        count += 1
      else:
        # if low < max_drop_price:
        #   aggregate_gain.append(-1 * max_drop)
        # else:
        frac_return = (close - bet_price) / bet_price
        aggregate_gain.append(frac_return)

      returnAmount = earned_amount * frac_return
      former_amount = earned_amount
      earned_amount = earned_amount + (earned_amount * frac_return)

      if earned_amount < 0:
        earned_amount = 0

      logger.info(f"\t{former_amount} + {returnAmount} = {earned_amount}")

    return count, earned_amount, aggregate_gain


