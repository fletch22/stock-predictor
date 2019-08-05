import os
import time
from unittest import TestCase

import findspark
import pandas as pd
from pyspark import SparkContext, SparkFiles
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.types import LongType, DoubleType, StructField, StructType, FloatType

import config
from config import logger_factory
from services import chart_service, file_services
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService

logger = logger_factory.create_logger(__name__)

class TestPySpark(TestCase):

  def test_connection(self):
    # Arrange
    findspark.init()
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    # Act
    df: DataFrame = sqlContext.read.format('com.databricks.spark.csv')\
      .options(header='true', inferschema='true').load(config.constants.SHAR_EQUITY_PRICES_SHORT)

    # process_rows_udf = udf(process_rows)
    #
    # df.groupBy("ticker").apply(process_rows_udf)

    # Assert
    sc.stop()

  def test_get_samples(self):
    # min_price = 5.0
    # amount_to_spend = 25000
    # trading_days_span = 1000
    # min_samples = 240000
    # pct_gain_sought = 1.0
    # save_dir = ""
    #
    # par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "testPySpark")
    # package_path = file_services.create_unique_folder(par_dir, "process")
    #
    # graph_dir = os.path.join(package_path, "graphed")
    # os.makedirs(graph_dir, exist_ok=True)
    #
    # output_dir = os.path.join(config.constants.CACHE_DIR, "spark_test")
    # os.makedirs(output_dir, exist_ok=True)
    # df_g_filtered = StockService._get_and_prep_equity_data(amount_to_spend, trading_days_span, min_price, SampleFileTypeSize.LARGE)
    #
    # logger.info(f"Num with symbols after group filtering: {df_g_filtered.shape[0]}")
    #
    # sample_info = StockService.get_sample_infos(df_g_filtered, trading_days_span, min_samples)
    #
    # findspark.init()
    # sc = SparkContext.getOrCreate()
    # sc.setLogLevel("INFO")
    # print(sc._jsc.sc().uiWebUrl().get())
    #
    # symbol_arr = []
    # for symbol in sample_info.keys():
    #   s_dict = sample_info[symbol]
    #   s_dict['symbol'] = symbol
    #   s_dict['trading_days_span'] = trading_days_span
    #   s_dict['pct_gain_sought'] = pct_gain_sought
    #   s_dict['save_dir'] = graph_dir
    #   symbol_arr.append(s_dict)
    #
    # rdd = sc.parallelize(symbol_arr)
    #
    # rdd.foreach(process_sample_info)
    #
    # sc.stop()

    # Assert
    assert(len(sample_info) >= min_samples)

def process_sample_info(symbol_info):
  symbol = symbol_info["symbol"]
  logger.info(f"Processing {symbol}...")
  offsets = symbol_info["offsets"]
  trading_days_span = symbol_info['trading_days_span']
  pct_gain_sought = symbol_info['pct_gain_sought']
  save_dir = symbol_info["save_dir"]

  df = EquityUtilService.get_df_from_ticker_path(symbol, True)

  for start_offset in offsets:
    df_offset = df.tail(df.shape[0] - start_offset).head(trading_days_span)
    df_tailed = df_offset.tail(2)

    close_price = df_tailed.iloc[-2]['close']
    high_price = df_tailed.iloc[-1]['high']

    logger.info(f"Ticker: -2: {df_tailed.iloc[-2]['ticker']}; -1 {df_tailed.iloc[-2]['ticker']} BuyPrice: {close_price} SellPrice: {high_price}")
    pct_gain = ((high_price - close_price) / close_price) * 100

    logger.info(f"Gain: {pct_gain}")

    bet_date_str = df_offset["date"].tolist()[-1]
    category = "1" if pct_gain >= pct_gain_sought else "0"

    chart_service.save_data_as_chart(symbol, category, df_offset, bet_date_str, save_dir, translate_save_path_hdfs=True)



  



