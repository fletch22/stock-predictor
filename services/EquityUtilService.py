import os

import pandas as pd
from pyspark import SparkFiles

import config
from config.logger_factory import logger_factory
from services import file_services
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService

logger = logger_factory.create_logger(__name__)

class EquityUtilService:

  @classmethod
  def split_shar_equity_to_ticker_files(cls, sample_size: SampleFileTypeSize=SampleFileTypeSize.LARGE):

    if sample_size == SampleFileTypeSize.LARGE:
      shar_file_path = config.constants.SHAR_EQUITY_PRICES
    else:
      shar_file_path = config.constants.SHAR_EQUITY_PRICES_SHORT

    df = pd.read_csv(shar_file_path)

    unique_symbols = df["ticker"].unique().tolist()

    symbols_extracted = []
    for ndx, sym in enumerate(unique_symbols):
      df_symbol = df[df['ticker'] == sym]

      df_symbol.to_csv(cls.get_ticker_path(sym), index=False)
      logger.info(f"Extracted and wrote symbol '{sym}'; {len(unique_symbols) - (ndx + 1)} remain.")
      symbols_extracted.append(sym)

    return symbols_extracted

  @classmethod
  def get_ticker_path(cls, symbol):
    return os.path.join(config.constants.SHAR_SPLIT_EQUITY_PRICES_DIR, f"{symbol}.csv")

  @classmethod
  def get_df_from_ticker_path(cls, symbol, translate_to_hdfs_path=False):
    symbol_path = cls.get_ticker_path(symbol)
    if translate_to_hdfs_path:
      symbol_path = SparkFiles.get(symbol_path)

    return pd.read_csv(symbol_path)

  @classmethod
  def find_missing_days(cls):
    df = StockService.get_shar_equity_data(SampleFileTypeSize.SMALL)
    df_sorted = df.sort_values(by=['date'], inplace=False)

    start_date_str = 

    file_services.walk(config.constants.)
