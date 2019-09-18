import os
import random
import statistics
from datetime import datetime, timedelta

import pandas as pd
from pyspark import SparkFiles

from config.logger_factory import logger_factory
from services import file_services, eod_data_service
from services.Eod import Eod
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class EquityUtilService:

  @classmethod
  def get_df_from_ticker_path(cls, symbol, translate_to_hdfs_path=False):
    symbol_path = file_services.get_eod_ticker_file_path(symbol)
    if translate_to_hdfs_path:
      symbol_path = SparkFiles.get(symbol_path)

    df = None
    if os.path.exists(symbol_path):
      df = pd.read_csv(symbol_path)
      df.sort_values(by=['date'], inplace=True)

    return df

  @classmethod
  def calculate_category(cls, df: pd.DataFrame, pct_gain_sought: float):
    df_tailed = df.tail(2)

    bet_date_str = df.iloc[-2]['date']
    bet_close_price = df.iloc[-2][Eod.CLOSE]
    yield_high_price = df_tailed.iloc[-1]['high']
    yield_date_str = df_tailed.iloc[-1]["date"]

    logger.info(f"{df_tailed.iloc[-2]['ticker']}; BuyPrice: {bet_close_price}; HighPrice: {yield_high_price}; bet date: {bet_date_str}; Yield date: {yield_date_str}")
    pct_gain = ((yield_high_price - bet_close_price) / bet_close_price) * 100

    return "1" if pct_gain >= pct_gain_sought else "0", yield_date_str

  @classmethod
  def filter_equity_basic_criterium(cls, amount_to_spend: float, num_days_avail: int, min_price: float, ticker_group: pd.DataFrame, volatility_min:float=1000):
    df = ticker_group.sort_values(by='date')
    first_row = df.iloc[0]
    start_day_volume = first_row['volume']
    last_row = df.iloc[-1]
    yield_day_volume = last_row['volume']
    close_price = last_row[Eod.CLOSE]
    shares_bought = amount_to_spend / close_price

    std = 0
    if df.shape[0] > 1:
      std = statistics.stdev(df['close'].values.tolist())

    # Shares bought should be greater than .01% of the yield day's volume.
    # Shares bought should be greater than .005% of the start span day's volume. So
    return df.shape[0] > num_days_avail \
           and close_price >= min_price \
           and shares_bought > (.0001 * yield_day_volume) \
           and start_day_volume > (yield_day_volume / 2) \
           and std < volatility_min

  @classmethod
  def select_single_day_equity_data(cls, yield_date: datetime, trading_days_avail: int, min_price: float, amount_to_spend: float, volatility_min: float):
    yield_date_str = date_utils.get_standard_ymd_format(yield_date)

    earliest_date = yield_date - timedelta(days=(trading_days_avail // 253 * 365) + 500)

    earliest_date_str = date_utils.get_standard_ymd_format(earliest_date)
    logger.info(f"Earliest date in data set: {earliest_date_str}")

    # get merged data
    df = eod_data_service.get_todays_merged_shar_data()
    logger.info(f"Got merged data. Earliest date: {df.iloc[0, :]['date']}; latest date: {df.iloc[-1, :]['date']}")
    df_date_filtered = df[(df['date'] >= earliest_date_str) & (df['date'] <= yield_date_str)]

    df_traded_on_date = cls.filter_by_traded_on_date(df_date_filtered, yield_date)
    df_min_filtered = df_traded_on_date.groupby('ticker').filter(lambda x: cls.filter_equity_basic_criterium(amount_to_spend, trading_days_avail, min_price, x, volatility_min=volatility_min))

    return df_min_filtered

  @classmethod
  def filter_by_traded_on_date(cls, df: pd.DataFrame, trade_date: datetime):
    df.sort_values(by=['date'], inplace=True)
    trade_date_str = date_utils.get_standard_ymd_format(trade_date)

    def filter(x: pd.DataFrame):
      last_date_str = x.iloc[-1, :]['date']
      return trade_date_str == last_date_str

    return df.groupby('ticker').filter(lambda x: filter(x))

  @classmethod
  def filter_high_variability(cls, file_paths, translate_paths_for_hdfs: bool=True):

    def get_low_variability(fp):
      category_actual, symbol, date_str = cls.get_info_from_file_path(fp)
      df = EquityUtilService.get_df_from_ticker_path(symbol, translate_paths_for_hdfs)
      df = df[df['date'] < '2018-12-31']
      std = statistics.stdev(df['close'].values.tolist())
      return std < 24.0

    return list(filter(get_low_variability, file_paths))

  @classmethod
  def get_info_from_file_path(cls, file_path: str):
    basename = os.path.basename(file_path)
    parts = basename.split("_")
    cat_actual = parts[0]
    symbol = parts[1]  # .replace("-", "_")
    date_str = parts[2].split('.')[0]

    return cat_actual, symbol, date_str




