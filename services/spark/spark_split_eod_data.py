from typing import Union

import findspark
import math
from ipython_genutils.py3compat import xrange
from pyspark import SparkContext, SparkFiles

import config
import pandas as pd

from config import logger_factory
from services import file_services

logger = logger_factory.create_logger(__name__)

def use_spark_to_split(df_sorted: pd.DataFrame):
  num_slices = 4
  symbols_chunked = transform_symbols_to_spark_request(df_sorted, num_slices)

  do_spark(symbols_chunked, num_slices)

  return symbols_chunked

def transform_symbols_to_spark_request(df: pd.DataFrame, num_slices: Union[int, None]):
  symbols = df["ticker"].unique().tolist()

  sublist_size = math.ceil(len(symbols) / num_slices)
  logger.info(f"Will divide symbols into sublists of size: {sublist_size}")
  symbols_chunked = [symbols[x:x + sublist_size] for x in xrange(0, len(symbols), sublist_size)]

  return symbols_chunked


def do_spark(spark_arr, num_slices):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(spark_arr, numSlices=num_slices)
  rdd.foreach(process)

  sc.stop()

def process(symbols):
  import pandas as pd
  from config import logger_factory

  logger = logger_factory.create_logger(__name__)

  file_path_spark = SparkFiles.get(config.constants.SHAR_EQUITY_PRICES_MERGED)
  df = pd.read_csv(file_path_spark)

  df_select_symbols = df.groupby('ticker').filter(lambda x: x.iloc[0,:]['ticker'] in symbols)
  for s in symbols:
    df_symbol = df_select_symbols[df_select_symbols['ticker'] == s]
    logger.info(f"Splitting for sybmol {s}.")
    output_path = SparkFiles.get(file_services.get_eod_ticker_file_path(s))
    df_symbol.to_csv(output_path, index=False)