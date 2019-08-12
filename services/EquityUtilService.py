import os
from datetime import datetime, timedelta

import pandas as pd
from pyspark import SparkFiles

import config
from config.logger_factory import logger_factory
from services import file_services, stock_download_service, test_spark
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class EquityUtilService:

  @classmethod
  def split_shar_equity_to_ticker_files(cls):
    df_sorted = EquityUtilService.get_todays_merged_shar_data()
    stock_infos = test_spark.use_spark_to_split(df_sorted)
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

