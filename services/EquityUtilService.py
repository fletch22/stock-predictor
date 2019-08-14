import os
from datetime import datetime, timedelta

import pandas as pd
from pyspark import SparkFiles

import config
from config.logger_factory import logger_factory
from services import file_services, stock_download_service, spark_split_files
from services.Eod import Eod
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class EquityUtilService:

  @classmethod
  def split_shar_equity_to_ticker_files(cls):
    df_sorted = EquityUtilService.get_todays_merged_shar_data()
    stock_infos = spark_split_files.use_spark_to_split(df_sorted)
    return stock_infos

  @classmethod
  def get_df_from_ticker_path(cls, symbol, translate_to_hdfs_path=False):
    symbol_path = file_services.get_ticker_filename(symbol)
    if translate_to_hdfs_path:
      symbol_path = SparkFiles.get(symbol_path)

    return pd.read_csv(symbol_path)

  @classmethod
  def get_supplemental_filename(cls, start_date: datetime, end_date: datetime):
    return f"supp_{date_utils.get_standard_ymd_format(start_date)}_{date_utils.get_standard_ymd_format(end_date)}.csv"

  @classmethod
  def find_and_download_missing_days(cls, df: pd.DataFrame, until_date: datetime):
    df_sorted = df.sort_values(by=['date'], inplace=False)

    end_date_str = df_sorted.iloc[-1, :]["date"]
    end_date = date_utils.parse_datestring(end_date_str)

    next_date = end_date + timedelta(days=1)

    missing_dates = []
    while next_date <= until_date:
      next_plus_one = next_date + timedelta(days=1)
      filename = cls.get_supplemental_filename(next_date, next_plus_one)
      print(f"Getting next filename: {filename}")
      file_path = os.path.join(config.constants.SHAR_EQUITY_PRICES_DIR, filename)

      if not os.path.exists(file_path):
        stock_download_service.download_sep(next_date, next_plus_one, file_path)
        missing_dates.append(next_date)

      next_date = next_date + timedelta(days=1)

    return missing_dates

  @classmethod
  def get_shar_equity_data(cls, sample_file_size: SampleFileTypeSize = SampleFileTypeSize.LARGE) -> pd.DataFrame:
    file_path = config.constants.SHAR_EQUITY_PRICES
    if sample_file_size == SampleFileTypeSize.SMALL:
      file_path = config.constants.SHAR_EQUITY_PRICES_SHORT
    df = pd.read_csv(file_path)
    return df.sort_values(by=['date'], inplace=False)

  @classmethod
  def get_todays_merged_shar_data(cls):
    # Test if file exists
    merged_path = config.constants.SHAR_EQUITY_PRICES_MERGED
    if os.path.exists(merged_path) and file_services.file_modified_today(merged_path):
      df_merged = pd.read_csv(merged_path).sort_values(by=['date'])
    else:
      df = cls.get_shar_equity_data()
      df_merged = cls.merge_shar_equity_price_data(df)

      df_merged.to_csv(config.constants.SHAR_EQUITY_PRICES_MERGED, index=False)

    return df_merged

  @classmethod
  def merge_shar_equity_price_data(self, df_base: pd.DataFrame):
    files = file_services.walk(config.constants.SHAR_EQUITY_PRICES_DIR)

    supp_files = [f for f in files if os.path.basename(f).startswith("supp_")]
    logger.info(f"Found {len(supp_files)} supplemental files.")

    sort_crit = ['ticker', 'date']
    df_merged = df_base.sort_values(by=sort_crit, inplace=False)
    for supp in supp_files:
      logger.info(f"Adding {os.path.basename(supp)}.")
      df_supp = pd.read_csv(supp)
      df_supp.sort_values(by=sort_crit, inplace=True)

      df_merged = pd.concat([df_merged, df_supp], ignore_index=False)

      logger.info(f"df_merged size: {df_merged.shape[0]}")

    df_merged.drop_duplicates(sort_crit, inplace=True)

    logger.info(f"df_unduped size: {df_merged.shape[0]}")

    df_merged.sort_values(by=['date'], inplace=True)

    return df_merged

  @classmethod
  def calculate_category(cls, df: pd.DataFrame, pct_gain_sought: float):
    df_tailed = df.tail(2)

    bet_close_price = df.iloc[-2][Eod.CLOSE]
    yield_high_price = df_tailed.iloc[-1]['high']
    yield_date_str = df_tailed.iloc[-1]["date"]

    logger.info(f"Ticker: -2: {df_tailed.iloc[-2]['ticker']}; -1 {df_tailed.iloc[-2]['ticker']} BuyPrice: {bet_close_price}; HighPrice: {yield_high_price}; Yield date: {yield_date_str}")
    pct_gain = ((yield_high_price - bet_close_price) / bet_close_price) * 100

    return ("1" if pct_gain >= pct_gain_sought else "0", yield_date_str)

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
    df = EquityUtilService.get_todays_merged_shar_data()
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
