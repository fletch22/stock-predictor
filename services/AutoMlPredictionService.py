import os
import random
import statistics
from datetime import datetime
from statistics import mean
from typing import List, Dict

from google.cloud import automl_v1beta1 as automl

import config
from calculators.EodCalculator import EodCalculator
from config import logger_factory
from services import file_services, SlackRealtimeMessageService
from services.Eod import Eod
from services.EquityUtilService import EquityUtilService
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
from services.RedisService import RedisService
from services.SlackService import SlackService
from services.StockService import StockService
from services.TickerService import TickerService
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

  # Example Returns:  List of row = {
  #     "symbol": symbol,
  #     "image_path": image_path,
  #     "date": None,
  #     "category_actual": category_actual,
  #     "category_predicted": None,
  #     "score": None,
  #     "model_full_id": model_full_id
  #   }
  def predict(self, task_dir: str, image_dir: str, min_price: float, min_volume: float, std_min: float, max_files=-1, purge_cached: bool = False, start_sample_date: datetime=None):
    file_infos = file_services.truncate_older(image_dir)
    file_paths = [fi['filepath'] for fi in file_infos]

    random.shuffle(file_paths, random.random)

    if max_files > -1 and len(file_paths) > max_files:
      file_paths = file_paths[:max_files]

    tick_exch = TickerService.get_tickers_list()
    tick_rt = RealtimeEquityPriceService.get_ticker_list_from_file()
    valid_tickers = list(set(tick_exch).intersection(tick_rt))

    # Act
    image_info = []
    for f in file_paths:
      category_actual, symbol, yield_date_str = EquityUtilService.get_info_from_file_path(f)
      yield_date = date_utils.parse_std_datestring(yield_date_str)

      df = EquityUtilService.get_df_from_ticker_path(symbol=symbol, translate_to_hdfs_path=False)
      df_dt_filtered = StockService.filter_dataframe_by_date(df=df, start_date=None, end_date=yield_date)
      df_curt = df_dt_filtered.iloc[-1000:, :]
      std = 0
      if df_curt.shape[0] > 1:
        std = statistics.stdev(df_curt['close'].values.tolist())

      bet_day_row = df_curt.iloc[-2,:]
      volume = bet_day_row["volume"]
      bet_price = bet_day_row['close']

      if start_sample_date is None:
        start_sample_date = date_utils.parse_std_datestring('1000-01-01')

      if bet_price > min_price and volume > min_volume and symbol in valid_tickers and yield_date > start_sample_date and std < std_min:
        logger.info(f"{symbol} using yield date {yield_date_str}")
        image_info.append({"category_actual": category_actual,
                           "symbol": symbol,
                           "file_path": f,
                           "score_threshold": self.score_threshold,
                           "short_model_id": self.short_model_id,
                           "purged_cached": purge_cached,
                           })

    found_predictions, not_found_image_infos = self.get_in_cache(image_info)

    logger.info(f"Will request AutoML predictions for {len(not_found_image_infos)} items.")
    spark_results = spark_predict.get_spark_results(not_found_image_infos, task_dir)

    return spark_results + found_predictions

  def predict_and_calculate(self, task_dir: str, image_dir: str, sought_gain_frac: float, min_price: float, min_volume: float, std_min: float, max_files: int=-1, purge_cached: bool = False, start_sample_date: datetime=None):
    results = self.predict(task_dir=task_dir, image_dir=image_dir,
                          min_price=min_price, min_volume=min_volume,
                           std_min=std_min, max_files=max_files,
                           purge_cached=purge_cached, start_sample_date=start_sample_date)

    return self.calculate_mean_accuracy(results=results, sought_gain_frac=sought_gain_frac, min_price=min_price)

  def get_in_cache(self, image_info: List[Dict]):

    redis_service = RedisService()

    found_predictions = []
    not_found_ii = []
    for ii in image_info:
      image_path = ii['file_path']
      short_model_id = ii['short_model_id']
      key_pred = self.get_prediction_cache_key(image_path, short_model_id)

      prediction = redis_service.read_as_json(key_pred)

      if prediction is not None:
        logger.info("Found prediction in Redis cache.")
        found_predictions.append(prediction)
      else:
        not_found_ii.append(ii)

    redis_service.close_client_connection()

    return found_predictions, not_found_ii

  @staticmethod
  def get_prediction_cache_key(image_path: str, short_model_id: str):
    return f"{short_model_id}_{image_path}"

  # Example results:  List of row = {
  #     "symbol": symbol,
  #     "image_path": image_path,
  #     "date": None,
  #     "category_actual": category_actual,
  #     "category_predicted": None,
  #     "score": None,
  #     "model_full_id": model_full_id
  #   }
  def calculate_mean_accuracy(self, results: List, sought_gain_frac: float, min_price: float):
    logger.info(f"Results: {len(results)}")

    unique_symbols = set()
    for r in results:
      unique_symbols.add(r["symbol"])
    logger.info(f"Unique symbols: {len(unique_symbols)}")

    results_filtered_1 = [d for d in results if d['category_predicted'] == "1"]
    logger.debug(f"Found category_predicted = 1: {len(results_filtered_1)}.")

    count_yielded = 0
    logger.debug("About to get shar equity data for comparison calcs.")
    aggregate_gain = []
    max_drop = 1.0
    invest_amount = 10000

    for r in results_filtered_1:
      eod_info = StockService.get_eod_of_date(r["symbol"], date_utils.parse_std_datestring(r["date"]))
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

      calc_result = EodCalculator.make_nova_calculation(aggregate_gain=aggregate_gain, score_threshold=self.score_threshold,
                                                   bet_price=bet_price,
                                                   category_actual=category_actual,
                                                   category_predicted=category_predicted,
                                                   open=open, high=high, low=low, close=close,
                                                   count=count_yielded, initial_investment_amount=invest_amount,
                                                   max_drop=max_drop, score=score,
                                                   sought_gain_frac=sought_gain_frac,
                                                   symbol=symbol, yield_date=date_utils.parse_std_datestring(yield_date_str),
                                                   min_price=min_price)

      # result = EodCalculator.make_sell_at_open_calc(aggregate_gain=aggregate_gain, score_threshold=self.score_threshold,
      #                                               bet_price=bet_price,
      #                                               open=open, high=high, low=low, close=close,
      #                                               count=count_yielded, initial_investment_amount=invest_amount,
      #                                               max_drop=max_drop, score=score,
      #                                               sought_gain_frac=sought_gain_frac,
      #                                               symbol=symbol, yield_date=date_utils.parse_std_datestring(yield_date_str),
      #                                               min_price=min_price)

      count_yielded, invest_amount, aggregate_gain, frac_gain = calc_result

      r['gain'] = frac_gain

    for r in results_filtered_1:
      if r['gain'] > 0:
        logger.info(f"{r['symbol']}; {r['date']}; {round(r['gain'] * 100, 5)}")

    total_samples = len(aggregate_gain)

    mean_frac = 0
    if total_samples > 0:
      mean_frac = mean(aggregate_gain)

      message = f"Accuracy: {count_yielded / total_samples}; score_threshold: {self.score_threshold}; total: {total_samples}; mean M: {mean_frac}; total: {invest_amount}"
      logger.info(message)
      slack_service = SlackService()
      slack_service.send_direct_message_to_chris(message)
    else:
      logger.info("No samples to calculate.")

    return mean_frac, results_filtered_1


