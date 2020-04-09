import os
import random
import statistics
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import numpy as np
from pyspark import SparkFiles

from categorical.BinaryCategoryType import BinaryCategoryType
from charts.ChartMode import ChartMode
from config.logger_factory import logger_factory
from services import file_services, eod_data_service
from services.Eod import Eod
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
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
  def calculate_category_and_yield_date(cls, df: pd.DataFrame, pct_gain_sought: float, chart_mode: ChartMode) -> (BinaryCategoryType, datetime):
    df_tailed = df.tail(2)

    yield_high_price = df_tailed.iloc[-1]['high']
    yield_date_str = df_tailed.iloc[-1]["date"]
    yield_date = date_utils.parse_std_datestring(yield_date_str)

    category = BinaryCategoryType.UNKNOWN
    if chart_mode == ChartMode.BackTest:
      row_bet_day = df.iloc[-2]
      bet_date_str = row_bet_day['date']
      bet_close_price = row_bet_day[Eod.CLOSE]

      logger.info(f"{df_tailed.iloc[-2]['ticker']}; BuyPrice: {bet_close_price}; HighPrice: {yield_high_price}; bet date: {bet_date_str}; Yield date: {yield_date_str}")
      pct_gain = ((yield_high_price - bet_close_price) / bet_close_price) * 100
      category = BinaryCategoryType.ONE if pct_gain >= pct_gain_sought else BinaryCategoryType.ZERO

    return category, yield_date

  @classmethod
  def filter_equity_basic_criterium(cls, min_volume:int, num_days_avail: int, min_price: float, ticker_group: pd.DataFrame, volatility_min:float=1000):
    df = ticker_group.sort_values(by='date')
    last_row = df.iloc[-1]
    close_price = last_row[Eod.CLOSE]

    volume_mean = 0
    if df.shape[0] > 10:
      volume_mean = df.iloc[-10,:]['volume'].mean()

    std = 0
    if df.shape[0] > 1:
      std = statistics.stdev(df[Eod.CLOSE].values.tolist())

    # Shares bought should be greater than .01% of the yield day's volume.
    # Shares bought should be greater than .005% of the start span day's volume. So
    return df.shape[0] > num_days_avail \
           and close_price >= min_price \
           and std < volatility_min \
           and volume_mean >= min_volume


  @classmethod
  def select_single_day_equity_data(cls, yield_date: datetime, trading_days_avail: int, min_price: float, min_volume: int, volatility_min: float):
    yield_date_str = date_utils.get_standard_ymd_format(yield_date)

    earliest_date = yield_date - timedelta(days=(trading_days_avail // 253 * 365) + 500)

    earliest_date_str = date_utils.get_standard_ymd_format(earliest_date)
    logger.info(f"Earliest date in data set: {earliest_date_str}")

    # get merged data
    df = eod_data_service.get_todays_merged_shar_data()
    logger.info(f"Got merged data. Earliest date: {df.iloc[0, :]['date']}; latest date: {df.iloc[-1, :]['date']}")
    df_date_filtered = df[(df['date'] >= earliest_date_str) & (df['date'] <= yield_date_str)]

    df_traded_on_date = cls.filter_by_traded_on_date(df_date_filtered, yield_date)
    df_min_filtered = df_traded_on_date.groupby('ticker').filter(lambda x: cls.filter_equity_basic_criterium(
      min_volume=min_volume, num_days_avail=trading_days_avail, min_price=min_price, ticker_group=x, volatility_min=volatility_min
    ))

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
      std = statistics.stdev(df[Eod.CLOSE].values.tolist())
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

  @classmethod
  def get_merged_mean_stdev(cls, end_date: datetime, trading_days_span: int, price_min: float=9999999999999):
    end_date_str = date_utils.get_standard_ymd_format(end_date)
    df = eod_data_service.get_todays_merged_shar_data()
    df.sort_values(by=['date'], inplace=True)
    df_dt_filtered = df[df['date'] <= end_date_str]

    agg_std = []

    def get_std(df, agg_std):
      if df.shape[0] > 1:
        df = df.iloc[-trading_days_span:, :]
        close_price = df.iloc[-1,:][Eod.CLOSE]
        if close_price > price_min:
          symbol = df.iloc[0]['ticker']
          std = statistics.stdev(df[Eod.CLOSE].values.tolist())
          agg_std.append([symbol, std])
          logger.info(f"Calc stdev for symbol {symbol}: {std}; price: {close_price}")

    df_dt_filtered.groupby('ticker').filter(lambda x: get_std(x, agg_std))

    df_agg = pd.DataFrame(agg_std, columns=['ticker', 'price'])

    # NOTE: 2019-09-14: chris.flesche: giving a haircut to remove outlier.
    return df_agg[df_agg['price'] < np.percentile(df_agg['price'], 99)]

  @classmethod
  def is_missing_today(cls, df: pd.DataFrame):
    today_str = date_utils.get_standard_ymd_format(datetime.now())
    df_now = df[df['date'] == today_str]

    symbols = df_now['ticker'].unique().tolist()

    return len(symbols) == 0

  @classmethod
  def add_realtime_price(cls, df: pd.DataFrame):
    symbols = df['ticker'].unique().tolist()

    logger.info(f"Symbols: {len(symbols)}: {symbols}")

    rt_equity_info_list = RealtimeEquityPriceService.get_and_clean_realtime_equities(symbols)

    row_list = []
    for ei in rt_equity_info_list:
      date_str = date_utils.get_standard_ymd_format(ei.last_trade_time)

      if np.isnan(ei.current_price):
        logger.info(f"Current price is nan. Removing symbol {ei.symbol} from dataframe.")
        df = df[~df['ticker'].isin([ei.symbol])]
      else:
        row = {'ticker': ei.symbol, 'date': date_str, 'open': ei.open,
               'high': ei.high_to_date, 'low': ei.low_to_date,
               'close': ei.current_price, 'volume': ei.volume_to_date, 'dividends': np.NaN, 'closeunadj': ei.current_price, 'lastupdated': date_str}
        row_list.append(row)

    df_today = pd.DataFrame(rt_equity_info_list)
    df_merged = pd.concat([df, df_today], ignore_index=True, sort=False)

    return df_merged