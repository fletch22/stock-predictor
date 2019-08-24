import os
import random
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
  def filter_equity_basic_criterium(cls, amount_to_spend: int, num_days_avail: int, min_price: float, ticker_group: pd.DataFrame):
    df_g_ticker_sorted = ticker_group.sort_values(by='date')
    first_row = df_g_ticker_sorted.iloc[0]
    start_day_volume = first_row['volume']
    last_row = df_g_ticker_sorted.iloc[-1]
    yield_day_volume = last_row['volume']
    close_price = last_row[Eod.CLOSE]
    shares_bought = amount_to_spend / close_price

    # Shares bought should be greater than .01% of the yield day's volume.
    # Shares bought should be greater than .005% of the start span day's volume. So
    return df_g_ticker_sorted.shape[0] > num_days_avail \
           and close_price >= min_price \
           and shares_bought > (.0001 * yield_day_volume) \
           and start_day_volume > (yield_day_volume / 2)

  @classmethod
  def select_single_day_equity_data(cls, yield_date: datetime):
    amount_to_spend = 25000
    num_days_available = 1000
    min_price = 5.0
    yield_date_str = date_utils.get_standard_ymd_format(yield_date)
    earliest_date = yield_date - timedelta(days=(num_days_available // 253 * 365) + 500)

    earliest_date_str = date_utils.get_standard_ymd_format(earliest_date)
    logger.info(f"Earliest date string: {earliest_date_str}")

    # get merged data
    df = eod_data_service.get_todays_merged_shar_data()
    logger.info(f"Got merged data. Earliest date: {df.iloc[0, :]['date']}; latest date: {df.iloc[-1, :]['date']}")
    df_date_filtered = df[(df['date'] >= earliest_date_str) & (df['date'] <= yield_date_str)]

    df_traded_on_date = cls.filter_by_traded_on_date(df_date_filtered, yield_date)
    df_min_filtered = df_traded_on_date.groupby('ticker').filter(lambda x: cls.filter_equity_basic_criterium(amount_to_spend, num_days_available, min_price, x))

    # file_path = os.path.join(config.constants.SHAR_EQUITY_PRICES_MED)
    # df_min_filtered = pd.read_csv(file_path)
    # df_temp = df_min_filtered[df_min_filtered['ticker'].isin(symbols[0:3])]
    # file_path = os.path.join(config.constants.SHAR_EQUITY_PRICES_MED)
    # df_temp.to_csv(file_path, index=False)

    return df_min_filtered

  @classmethod
  def filter_by_traded_on_date(cls, df: pd.DataFrame, trade_date: datetime):
    trade_date_str = date_utils.get_standard_ymd_format(trade_date)

    def filter(x: pd.DataFrame):
      last_date_str = x.iloc[-1, :]['date']
      return trade_date_str == last_date_str

    return df.groupby('ticker').filter(lambda x: filter(x))



