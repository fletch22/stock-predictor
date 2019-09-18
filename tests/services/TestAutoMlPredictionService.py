import os
import statistics
from unittest import TestCase

import pandas as pd
from stopwatch import Stopwatch

import config
from config import logger_factory
from services.AutoMlPredictionService import AutoMlPredictionService
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
from services.quality_verifiers import verify_folder_image_quality
from utils import date_utils
from utils.CalculationPackage import CalculationPackage
from utils.YieldCalculator import YieldCalculator

logger = logger_factory.create_logger(__name__)


class TestAutoMlPredictionService(TestCase):

  def test_predict_and_calculate(self):
    # Arrange
    # Without vol filter: Accuracy: 0.7396694214876033; total: 242; mean frac: -0.000855431676463572; total: 7269.49249471177
    # With filter volatility_min: 24; score_threshold = .55: Accuracy: 0.8090909090909091; total: 110; mean frac: 0.0023177019723329886; total: 12623.972695314542

    # short_model_id = "ICN7780367212284440071"  # fut_prof 68%
    # short_model_id = "ICN5723877521014662275" # closeunadj
    # short_model_id = "ICN2013469796611448097" # multi3 58.7%
    # short_model_id = "ICN7558148891127523672"  # multi3 57.5%
    # short_model_id = "ICN2174544806954869914"  # multi3 57.5%
    # short_model_id = "ICN2383257928398147071" # vol_eq 65%
    short_model_id = "ICN200769567050768296"  # voleq_rec Sept %

    # package_folder = "process_2019-09-09_22-14-56-0.37" # vanilla chart 8-15-2019: -0.004937923352530126
    # package_folder = "process_2019-09-14_11-47-31-845.11" # vol_eq
    package_folder = "process_2019-09-15_22-19-18-716.64"  # voleq_rec
    # package_folder = "process_2019-09-15_17-14-26-988.57" # 8-30 .0026
    # package_folder = "process_2019-09-15_17-10-30-37.45" # 8-29 .0030
    # package_folder = "process_2019-09-15_17-06-36-672.94"  # 8-28 .0050
    # package_folder = "process_2019-09-15_17-03-04-8.06" # 8-27 -0.0019
    # package_folder = "process_2019-09-15_14-02-07-600.9"  # 8-23  -0.0120
    # package_folder = "process_2019-09-15_13-58-15-340.5"  # 8-22   -0.0018
    # package_folder = "process_2019-09-15_13-54-19-578.46"  # 8-21   0.0023
    # package_folder = "process_2019-09-15_13-50-10-391.36"  # 8-20  -0.00198
    # package_folder = "process_2019-09-15_13-45-52-578.17"  # 8-19   0.0063
    # package_folder = "process_2019-09-15_13-39-32-870.98"  # 8-14  -0.0150
    # package_folder = "process_2019-09-15_16-59-07-574.45"  # 8-09 -0.0038
    # package_folder = "process_2019-09-15_16-55-31-225.99" # 8-08  0.0052
    # package_folder = "process_2019-09-15_16-51-55-456.24"  # 8-07 -0.00094
    # package_folder = "process_2019-09-15_16-48-14-245.38"  # 8-06 0.0063
    # package_folder = "process_2019-09-15_16-44-44-97.67"  # 8-05 -0.0196
    # package_folder = "process_2019-09-15_16-41-08-259.13"  # 8-02 -0.0057
    # package_folder = "process_2019-09-15_16-37-38-928.1"  # 8-01 -0.0021
    # package_folder = "process_2019-09-15_16-34-06-17.38"  # 7-31 -0.00096
    # package_folder = "process_2019-09-15_16-30-25-885.47"  # 7-30 0.0015
    # package_folder = "process_2019-09-15_16-26-47-851.12"  # 7-29 -0.0018
    # package_folder = "process_2019-09-15_16-23-03-394.13" # 7-26 0.00468
    # package_folder = "process_2019-09-15_16-19-05-587.39"  # 7-25 -0.0062
    # package_folder = "process_2019-09-15_16-15-23-25.45"  # 7-24  0.0022
    # package_folder = "process_2019-09-15_16-11-40-753.51"  # 7-23  0.00349
    # package_folder = "process_2019-09-15_16-07-42-827.66"  # 7-22  0.0015
    # package_folder = "process_2019-09-15_16-04-00-588.58"  # 7-19  -0.0027
    # package_folder = "process_2019-09-15_16-00-11-48.41"  # 7-18  -0.0012
    # package_folder = "process_2019-09-15_15-56-00-356.66"  # 7-17  -0.0022

    # package_folder = "process_2019-09-08_06-08-31-603.66" # 8-15-2019 24h multi3 57.5%
    # package_folder = "process_2019-08-06_22-46-56-230.65" # vanilla chart

    data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
    image_dir = os.path.join(data_cache_dir, "test_holdout")
    # image_dir = os.path.join(data_cache_dir, "graphed")

    sought_gain_frac = .01
    score_threshold = .50
    purge_cached = False

    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=score_threshold)

    # Act
    stopwatch = Stopwatch()
    stopwatch.start()
    auto_ml_service.predict_and_calculate(package_folder, image_dir, sought_gain_frac, max_files=10000, purge_cached=purge_cached)
    stopwatch.stop()

    logger.info(f"Elapsed time: {round(stopwatch.duration / 60, 2)} minutes")

    # Assert
    assert (True)

  def test_fast_calc(self):
    # scores_ICN2174544806954869914_2019-07-17_2019-08-15.csv
    # short_model_id = "ICN7780367212284440071"
    # short_model_id = "ICN2174544806954869914"
    short_model_id = "ICN2383257928398147071" # vol_eq_09_14_11_47_31_845_11 65%
    start_date_str = "2019-07-17"
    end_date_str = "2019-09-06"
    end_snip_str = "2019-09-06"
    score_threshold_low = .56
    # score_threshold_high = .62
    initial_investment_amount = 10000
    sought_gain_frac = .01
    max_price_drop_frac = None
    bets_per_day = 1

    file_path = os.path.join(config.constants.CACHE_DIR, f"scores_{short_model_id}_{start_date_str}_{end_date_str}.csv")

    df = pd.read_csv(file_path)

    agg_success_ratio = []
    agg_roi_pct = []
    for i in range(100):
      df = df.sample(frac=1).reset_index(drop=True)

      df_g = prefilter_bets(df, bets_per_day, score_threshold_low, end_snip_str)

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

      roi_pct = ((ongoing_balance_amt - initial_investment_amount) / initial_investment_amount) * 100
      agg_roi_pct.append(roi_pct)

      if count_predicted > 0:
        success_ratio = round(count_gainers / count_predicted, 2)
        agg_success_ratio.append(success_ratio)

        logger.info(f"Threshold: {score_threshold_low}; hits/total: {count_gainers}/{count_predicted} ({success_ratio}); roi: {roi_pct}%")

    if len(agg_roi_pct) > 0:
      logger.info(f"agg mean roi pct: {round(statistics.mean(agg_roi_pct), 2)}%; average success: {round(statistics.mean(agg_success_ratio), 2)}")
    else:
      logger.info("No results.")

  def test_quality_by_df(self):
    sought_gain_frac = .01
    start_date_str = '2019-08-13'
    end_date_str = '2019-08-14'
    merged_path = config.constants.SHAR_EQUITY_PRICES_MERGED
    df = pd.read_csv(merged_path).sort_values(by=['date'])

    df = df[(df['date'] >= start_date_str) & (df['date'] <= end_date_str)]

    def filter(df):
      df.sort_values(by="date", inplace=True)

      bet_row = df.iloc[0, :]
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
