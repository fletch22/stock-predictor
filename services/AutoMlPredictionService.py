import os
import random
from statistics import mean

from google.cloud import automl_v1beta1 as automl

import config
from config import logger_factory
from services import file_services
from services.spark import spark_predict
from services.Eod import Eod
from services.StockService import StockService
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

  def predict_and_calculate(self, image_dir: str, sought_gain_frac: float, max_files=-1, purge_cached:bool=False):
    file_paths = file_services.walk(image_dir)
    random.shuffle(file_paths, random.random)

    if max_files > -1 and len(file_paths) > max_files:
      file_paths = file_paths[:max_files]

    # Act
    image_info = []
    for f in file_paths:
      basename = os.path.basename(f)
      basename_parts = basename.split("_")
      category_actual = basename_parts[0]
      symbol = basename_parts[1]

      image_info.append({"category_actual": category_actual,
                         "symbol": symbol,
                         "file_path": f,
                         "score_threshold": self.score_threshold,
                         "short_model_id": self.short_model_id,
                         "purged_cached": purge_cached
                         })

    results = spark_predict.get_spark_results(image_info)

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
      yield_date = r["date"]

      count_yielded, invest_amount, aggregate_gain = self.calc_frac_gain(aggregate_gain, bet_price, category_actual, category_predicted, close, count_yielded, high, invest_amount, low, max_drop, score, sought_gain_frac, symbol, yield_date)

    for n in aggregate_gain:
      logger.info(n)

    total_samples = len(aggregate_gain)
    logger.info(f"Accuracy: {count_yielded / total_samples}; total: {total_samples}; mean frac: {mean(aggregate_gain)}; total: {invest_amount}")

  def calc_frac_gain(self, aggregate_gain, bet_price, category_actual, category_predicted, close, count, high, initial_investment_amount, low, max_drop, score, sought_gain_frac, symbol, yield_date):
    earned_amount = initial_investment_amount

    if bet_price is None:
      return count, earned_amount, aggregate_gain

    logger.debug(f"Score: {score}; score_threshold: {self.score_threshold}")
    max_drop_price = bet_price - (max_drop * bet_price)
    logger.info(f"bet_price: {bet_price}; high: {high}; low: {low}; close: {close}; max_drop_price: {max_drop_price}; ")
    if score >= self.score_threshold:
      if category_actual == category_predicted:
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

      logger.info(f"Symbol: {symbol}; {former_amount} + {returnAmount} = {earned_amount}")

    return count, earned_amount, aggregate_gain
