import os
from datetime import timedelta
import random
from unittest import TestCase

import math
import pandas as pd
from stopwatch import Stopwatch
from statistics import mean

import config
from config import logger_factory
from services import file_services
from services.AutoMlPredictionService import AutoMlPredictionService
from services.EquityUtilService import EquityUtilService
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
from services.quality_verifiers import verify_folder_image_quality
from utils import date_utils
from utils.CalculationPackage import CalculationPackage
from utils.YieldCalculator import YieldCalculator
import statistics

logger = logger_factory.create_logger(__name__)



class TestAutoMlPredictionService(TestCase):

  def test_predict_and_calculate(self):
    # Arrange
    short_model_id = "ICN7780367212284440071"  # fut_prof 68%
    # short_model_id = "ICN5723877521014662275" # closeunadj
    # package_folder = "process_2019-08-04_09-08-53-876.24"
    # package_folder = "process_2019-08-06_21-07-03-123.23"
    # package_folder = "process_2019-08-07_08-27-02-182.53"
    # package_folder = "process_2019-08-07_21-01-14-275.25"
    # package_folder = "process_2019-08-08_21-28-43-496.67"
    # data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
    # image_dir = os.path.join(data_cache_dir, "test_holdout")

    # package_folder = "tsczuz__2019-08-13_07-44-49-19.99"  # 2019-07-17 .56 -> -.36
    # package_folder = "tsczuz__2019-08-13_08-02-50-934.15"  # 2019-07-18 .56 -> -.07
    # package_folder = "tsczuz__2019-08-13_23-05-25-314.13"  # 2019-07-19 .56 -> .1
    # package_folder = "tsczuz__2019-08-13_08-19-29-765.36"  # 2019-07-22 .56 -> .14
    # package_folder = "tsczuz__2019-08-13_08-34-11-93.47"  # 2019-07-23 .56 -> .39
    # package_folder = "tsczuz__2019-08-13_19-33-09-981.39"  # 2019-07-24 .56 -> .48
    # package_folder = "tsczuz__2019-08-13_19-50-20-163.87"  # 2019-07-25 .56 -> -.09
    # package_folder = "tsczuz__2019-08-14_20-12-46-174.24"  # 2019-07-26 .56 -> .31
    # package_folder = "tsczuz__2019-08-13_20-15-02-219.27"  # 2019-07-29 .56 -> -.18
    # package_folder = "tsczuz__2019-08-13_20-32-05-80.24" #2019-07-30 .56 -> .33
    # package_folder = "tsczuz__2019-08-13_21-05-59-524.71"  # 2019-07-31 .56 -> .31
    # package_folder = "tsczuz__2019-08-13_21-16-52-909.21" # 2019-08-01 .56 -> .08
    # package_folder = "tsczuz__2019-08-14_20-54-17-876.95" # 2019-08-02 .56 -> -.11
    # package_folder = "tsczuz__2019-08-14_22-01-55-582.07"  # 2019-08-05 .56 -> -.31
    # package_folder = "tsczuz__2019-08-14_22-27-57-370.67"  # 2019-08-06 .56 -> .89
    # package_folder = "tsczuz__2019-08-14_22-41-04-602.12"  # 2019-08-07 .56 -> .03
    # package_folder = "tsczuz__2019-08-14_22-56-11-292.63"  # 2019-08-08 .56 -> .68
    # package_folder = "tsczuz__2019-08-14_23-08-01-480.4"  # 2019-08-09 .56 -> -.71
    package_folder = "tsczuz__2019-08-16_15-37-00-100.37"  # 2019-08-12 .56 -> -.98
    # package_folder = ""  # 2019-08-13 .56 -> -.47
    # package_folder = ""  # 2019-08-13 .56 -> -.52

    data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "test_one_day")
    image_dir = os.path.join(data_cache_dir, package_folder)
    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=.56)
    sought_gain_frac = .01

    # Act
    stopwatch = Stopwatch()
    stopwatch.start()
    auto_ml_service.predict_and_calculate(image_dir, sought_gain_frac, max_files=3000, purge_cached=False)
    stopwatch.stop()

    logger.info(f"Elapsed time: {round(stopwatch.duration / 60, 2)} minutes")

    # Assert
    assert (True)

  def test_fast_calc(self):
    short_model_id = "ICN7780367212284440071"
    start_date_str = "2019-07-17"
    end_date_str = "2019-08-15"
    end_snip_str = "2019-08-15"
    score_threshold_low = .50
    score_threshold_high = .62
    initial_investment_amount = 10000
    sought_gain_frac = .02
    max_price_drop_frac = None

    file_path = os.path.join(config.constants.CACHE_DIR, f"scores_{short_model_id}_{start_date_str}_{end_date_str}.csv")

    df = pd.read_csv(file_path)

    agg_roi_pct = []
    for i in range(1000):
      df = df.sample(frac=1).reset_index(drop=True)

      df_g = prefilter_bets(df, 10, score_threshold_low, end_snip_str)

      df_g.sort_values(by=['date'], inplace=True)

      yield_calculator = YieldCalculator(score_threshold=score_threshold_low, initial_investment_amount=initial_investment_amount, sought_gain_frac=sought_gain_frac)

      def calc(row):
        calc_package = CalculationPackage()
        calc_package.symbol = row['symbol']
        calc_package.bet_price = row['bet_price']
        calc_package.high = row['high']
        calc_package.low = row['low']
        calc_package.close = row['close']
        calc_package.score = row['score']
        calc_package.date_str = row['date']
        calc_package.max_price_drop_frac = max_price_drop_frac

        category_predicted = str(row['category_predicted']).lower()
        category_actual = str(row['category_actual']).lower()
        if category_predicted != 'nan' and category_actual != 'nan':
          calc_package.category_predicted = str(int(float(category_predicted)))
          calc_package.category_actual = str(int(float(category_actual)))

          if calc_package.validate():
            yield_calculator.calc_frac_gain(calc_package)

      df_g.apply(lambda row: calc(row), axis=1)

      count_gainers = yield_calculator.count_gainers
      count_predicted = yield_calculator.count_predicted
      gains_map = yield_calculator.gains_map

      ongoing_balance_amt = initial_investment_amount

      date_keys = list(gains_map.keys())
      date_keys.sort()

      for date_str in date_keys:
        frac_returns = gains_map.get(date_str)

        daily_split_amt = ongoing_balance_amt / len(frac_returns)
        eod_result = 0
        for f in frac_returns:
          single_result = daily_split_amt + (daily_split_amt * f)
          eod_result += single_result

        ongoing_balance_amt = eod_result
        if ongoing_balance_amt <= 0:
          break

      roi_pct = ((ongoing_balance_amt - initial_investment_amount)/initial_investment_amount) * 100
      agg_roi_pct.append(roi_pct)

      success_ratio = round(count_gainers/count_predicted, 2)
      logger.info(f"Threshold: {score_threshold_low}; hits/total: {count_gainers}/{count_predicted} ({success_ratio}); roi: {roi_pct}%")

    logger.info(f"agg mean roi pct: {round(statistics.mean(agg_roi_pct), 2)}%")


  def test_quality_by_df(self):
    sought_gain_frac = .01
    start_date_str = '2019-08-13'
    end_date_str = '2019-08-14'
    merged_path = config.constants.SHAR_EQUITY_PRICES_MERGED
    df = pd.read_csv(merged_path).sort_values(by=['date'])

    df = df[(df['date'] >= start_date_str) & (df['date'] <= end_date_str)]

    def filter(df):
      df.sort_values(by="date", inplace=True)

      bet_row = df.iloc[0,:]
      symbol = bet_row['ticker']

      print(symbol)

      bet_price = bet_row['close']
      yield_row = df.iloc[1, :]
      high = yield_row['high']

      pct_gain = (high - bet_price) / bet_price
      cat_actual_df = "0" if pct_gain < sought_gain_frac else "1"

      bet_date = date_utils.parse_datestring(bet_row['date'])
      yield_date = date_utils.parse_datestring(yield_row['date'])

      price_data = RealtimeEquityPriceService.get_historical_price(symbol, start_date=bet_date, end_date=yield_date)

      if 'history' in price_data:
        history = price_data['history']

        bet_date_str = date_utils.get_standard_ymd_format(bet_date)
        yield_date_str = date_utils.get_standard_ymd_format(yield_date)
        if bet_date_str in history and yield_date_str in history:
          bet_date_data = history[bet_date_str]
          bet_price_h = float(bet_date_data['close'])

          yield_date_data = history[yield_date_str]
          high_h = float(yield_date_data['high'])

          pct_gain = (high_h - bet_price_h) / bet_price_h
          cat_actual_rt = "0" if pct_gain < sought_gain_frac else "1"

          print(f"bp: {bet_price_h}; high: {high_h}; g: {pct_gain}; dfcat: {cat_actual_df}; scat: {cat_actual_rt}")

    df.groupby("ticker").filter(lambda x: filter(x))

  def test_image_folder_quality(self):
      verify_folder_image_quality.verify()

def prefilter_bets(df: pd.DataFrame, num_bets_per_day: int, score_threshold_low: float, end_date_str: str):
  def top_values(x):
    x = x.dropna(axis='rows')
    x = x[x['category_predicted'] == 1]
    x = x[(x['score'] >= score_threshold_low)]
    x = x[x['date'] <= end_date_str]

    if x.shape[0] >= num_bets_per_day:
      x = x.tail(num_bets_per_day)
    return x
  return df.groupby('date').apply(lambda x: top_values(x)).reset_index(drop=True)



