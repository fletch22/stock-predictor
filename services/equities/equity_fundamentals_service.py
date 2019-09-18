import os
from datetime import datetime
from typing import Sequence, Dict

import math
from pyspark import SparkFiles
import pandas as pd
from stopwatch import Stopwatch

from config import logger_factory
from min_max.ListMinMaxer import ListMinMaxer
from services import file_services
from services.equities.EquityFundaDimension import EquityFundaDimension
from services.spark import spark_get_fundamentals
from utils import date_utils, math_utils

logger = logger_factory.create_logger(__name__)

def get_symbol_fundamental_data(symbol, bet_date, translate_to_hdfs: bool=False):
  file_path = get_symbol_file_path(symbol, translate_to_hdfs=translate_to_hdfs)

  df_sorted = None
  if os.path.exists(file_path):
    df = pd.read_csv(file_path)

    bet_date_str = date_utils.get_standard_ymd_format(bet_date)
    logger.debug(f"bet_date_str: {bet_date_str}")

    df_filtered: pd.DataFrame = df[(df['datekey'] <= bet_date_str)]

    df_fin_filtered = df_filtered[df_filtered['dimension'] == EquityFundaDimension.MostRecentQuarterly]
    if df_fin_filtered.shape[0] == 0:
      df_fin_filtered = df_filtered[(df_filtered['dimension'] == EquityFundaDimension.MostRecentTrailingYear)]
      if df_fin_filtered.shape[0] == 0:
        df_fin_filtered = df_filtered[(df_filtered['dimension'] == EquityFundaDimension.MostRecentAnnual)]

    df_sorted = df_fin_filtered.sort_values(by=['datekey'], ascending=False, inplace=False)

  return df_sorted

def get_column_value_in_last_row(df: pd.DataFrame, column_name: str):
  column_value = None
  if df.shape[0] > 0:
    column_value = df.iloc[0][column_name]

  return column_value

def get_symbol_file_path(symbol: str, translate_to_hdfs: bool=False):
  symbol = symbol if symbol != "goog" else "googl"

  file_path = file_services.get_fun_ticker_file_path(symbol)

  if translate_to_hdfs:
    file_path = SparkFiles.get(file_path)

  return file_path

def get_multiple_values(symbol: str, bet_date: datetime, desired_values: Sequence[str], df: pd.DataFrame=None, translate_to_hdfs=False) -> object:
  if df is None:
    df = get_symbol_fundamental_data(symbol, bet_date, translate_to_hdfs=translate_to_hdfs)

  result = {}
  for dv in desired_values:
    value = None
    if (df is not None):
      value = get_column_value_in_last_row(df, dv)
    else:
      logger.info(f"ERROR: {symbol} could not be retrieved from file system.")

    result[dv] = value

  return result

# Returns type example: [{'symbol': 'ibm', 'offset_info': {100: {'pe': 1.4, 'ev': 2.345}, 200: {'pe': 9.888, 'ev': 4.555}}}]
def get_scaled_sample_infos(sample_infos: Dict, package_path: str, trading_days_span: int, start_date: datetime, end_date: datetime, desired_fundamentals: Sequence[str]):
  spark_arr = spark_get_fundamentals.convert_to_spark_array(sample_infos, trading_days_span, start_date, end_date, desired_fundamentals)

  sample_infos_with_fundies = spark_get_fundamentals.do_spark(spark_arr, num_slices=1)

  logger.info(f"sifun: {sample_infos_with_fundies}")

  return get_scaled_fundamentals(sample_infos_with_fundies, package_path)

def _adjust_special_fundamentals(fundamental_key, fundamental_value):
  result = fundamental_value
  if fundamental_key == 'pe':
    if fundamental_value is not None:
      pe_min = -100
      pe_max = 200
      logger.info(f"Fun value before: {fundamental_value}")
      fundamental_value = pe_max if fundamental_value > pe_max else fundamental_value
      fundamental_value = pe_min if fundamental_value < pe_min else fundamental_value
      result = fundamental_value - pe_min # Adjust to zero based.
      logger.info(f"result value: {result}: pe: {pe_min}")

  return result

# Expects Type example: [{'symbol': 'ibm', 'offset_info': {100: {'pe': 14.15, 'ev': 198593033631.0}, 200: {'pe': 11.957, 'ev': 169182898289.0}}}]
def get_scaled_fundamentals(fundy_infos: Sequence, package_path: str):
  stopwatch = Stopwatch()
  stopwatch.start()
  logger.info("Scaling fundamentals...")

  # Special Adjustments
  for fund in fundy_infos:
    offset_infos = fund['offset_info']
    offsets = offset_infos.keys()
    for off in offsets:
      fun = offset_infos[off]
      for f_key in fun.keys():
        value = fun[f_key]
        value = _adjust_special_fundamentals(f_key, value)
        fun[f_key] = value if math_utils.is_neither_nan_nor_none(value) else None

  # min_maxs = _get_min_max_from_sample_infos(fundy_infos)
  list_min_maxer = create_min_maxer(fundy_infos, package_path=package_path)

  for fundy_key in fundy_infos:
    offset_infos = fundy_key['offset_info']
    offsets = offset_infos.keys()
    for off in offsets:
      fun = offset_infos[off]
      for f_key in fun.keys():
        # mm = min_maxs[f_key]
        # min = mm['min']
        # max = mm['max']
        value = fun[f_key]
        if value is not None:
          fun[f_key] = list_min_maxer.scale(value, f_key)
          # logger.info(f"Val: {value}; min: {min}; max: {max}")
          # value_scaled = math_utils.scale_value(value, min, max)
          # fun[f_key] = ((upper_bound - lower_bound) * value_scaled) + lower_bound

  stopwatch.stop()

  logger.info(f"Elapsed time scaling fundamentals: {stopwatch}.")
  return fundy_infos

# Expects Type example: [{'symbol': 'ibm', 'offset_info': {100: {'pe': 14.15, 'ev': 198593033631.0}, 200: {'pe': 11.957, 'ev': 169182898289.0}}}]
def create_min_maxer(fundy_infos: Sequence, package_path: str):
  fundies = {}
  for fundy_key in fundy_infos:
    offset_infos = fundy_key['offset_info']
    offsets = offset_infos.keys()
    for off in offsets:
      fun = offset_infos[off]
      for f_key in fun.keys():
        value = fun[f_key]
        if value is not None:
          if f_key not in fundies.keys():
            fundies[f_key] = []
          fundies[f_key].append(value)

  list_min_maxer = ListMinMaxer(package_path=package_path)
  list_min_maxer.persist(fundies)

  return list_min_maxer
