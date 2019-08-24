from datetime import datetime, timedelta

import findspark
from pyspark import SparkContext

from config import logger_factory
from services import chart_service
from services.Eod import Eod
from services.EquityUtilService import EquityUtilService
from services.StockService import StockService
from services.equities import equity_fundamentals_service
from services.equities.FullDataEquityFundamentalsService import FullDataEquityFundamentalsService

logger = logger_factory.create_logger(__name__)

def do_spark(stock_infos):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(stock_infos)

  rdd.foreach(spark_process)

  sc.stop()

def spark_process(stock_infos):
  symbol = stock_infos["symbol"]
  logger.info(f"Processing {symbol}...")
  offset_info = stock_infos["offset_info"]
  trading_days_span = stock_infos['trading_days_span']
  pct_gain_sought = stock_infos['pct_gain_sought']
  save_dir = stock_infos["save_dir"]
  start_date = stock_infos["start_date"]
  end_date = stock_infos["end_date"]

  df = EquityUtilService.get_df_from_ticker_path(symbol, True)
  df_sorted = df.sort_values(by=['date'], inplace=False)
  df_date_filtered = StockService.filter_dataframe_by_date(df_sorted, start_date, end_date)

  for offset in offset_info.keys():

    df_offset = df_date_filtered.tail(df_date_filtered.shape[0] - offset).head(trading_days_span)

    category, yield_date_str = EquityUtilService.calculate_category(df_offset, pct_gain_sought)

    fundies = offset_info[offset]
    logger.info(f"Fundies: {fundies}")

    chart_service.save_decorated_chart_to_filename(df_offset, fundy=fundies, category=category, symbol=symbol, yield_date_str=yield_date_str,
               save_dir=save_dir, translate_save_path_hdfs=False)

