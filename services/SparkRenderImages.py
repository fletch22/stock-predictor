import os
from datetime import datetime

import findspark
from pyspark import SparkContext, SparkFiles

import config
from config import logger_factory

import pandas as pd

from services import file_services, chart_service
from services.EquityUtilService import EquityUtilService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class SparkRenderImages():

  @classmethod
  def render_train_test_day(cls, yield_date: datetime, pct_gain_sought:float, num_days_to_sample: int):
    df_min_filtered = EquityUtilService.select_single_day_equity_data(yield_date)

    image_dir = SparkRenderImages.render_df(df_min_filtered, pct_gain_sought, num_days_to_sample, yield_date)

    return df_min_filtered, image_dir

  @classmethod
  def render_df(self, df_min_filtered: pd.DataFrame, pct_gain_sought: float, num_days_to_sample: int, yield_date: datetime):
    symbols = df_min_filtered["ticker"].unique().tolist()
    logger.info(f"slen {len(symbols)} {','.join(symbols)}")

    parent_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "test_one_day")
    save_dir = file_services.create_unique_folder(parent_dir, "tsczuz_")

    csv_path = os.path.join(save_dir, 'selection_data.csv')
    df_min_filtered.to_csv(csv_path, index=False)

    symbols_package = []
    for s in symbols:
      symb_pck = {
        "symbol": s,
        "yield_date": yield_date,
        "num_days_to_sample": num_days_to_sample,
        "save_dir": save_dir,
        "pct_gain_sought": pct_gain_sought
      }
      symbols_package.append(symb_pck)

    logger.info(f"Have {len(symbols)} symbols.")

    do_spark(symbols_package)

    return save_dir

def do_spark(spark_arr, num_slices=None):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(spark_arr, num_slices)
  rdd.foreach(process)

  sc.stop()

def process(symbol_info):
  import pandas as pd
  from config import logger_factory

  logger = logger_factory.create_logger(__name__)

  symbol = symbol_info["symbol"]

  logger.info(f"Processing {symbol}")

  yield_date = symbol_info["yield_date"]
  yield_date_str = date_utils.get_standard_ymd_format(yield_date)
  num_days_to_sample = symbol_info["num_days_to_sample"]
  save_dir = symbol_info["save_dir"]
  pct_gain_sought = symbol_info["pct_gain_sought"]

  split_file_path = os.path.join(config.constants.SHAR_SPLIT_EQUITY_PRICES_DIR, f"{symbol}.csv")
  file_path_spark = SparkFiles.get(split_file_path)
  df = pd.read_csv(file_path_spark)

  df_dt_filtered = df[df['date'] <= date_utils.get_standard_ymd_format(yield_date)]
  df_dt_filtered.sort_values(by=['date'], inplace=True)
  df = df_dt_filtered.tail(num_days_to_sample)

  category, _ = EquityUtilService.calculate_category(df, pct_gain_sought)

  chart_service.save_data_as_chart(symbol, category, df, yield_date_str, save_dir, translate_save_path_hdfs=True)
