import os

import config
import pandas as pd

from config.logger_factory import logger_factory
from utils.CalculationPackage import CalculationPackage
from utils.YieldCalculator import YieldCalculator
import statistics

logger = logger_factory.create_logger(__name__)

def test_fast_calc(self):
  short_model_id = "ICN2383257928398147071"  # vol_eq_09_14_11_47_31_845_11 65%
  start_date_str = "2019-07-17"
  end_date_str = "2019-09-06"
  end_snip_str = "2019-09-06"
  score_threshold_low = .56
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