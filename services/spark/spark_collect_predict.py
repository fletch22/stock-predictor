import os
from datetime import timedelta

import findspark
import pandas as pd
from pyspark import SparkContext, SparkFiles

import config
from config import logger_factory
from services import file_services
from services.EquityUtilService import EquityUtilService
from services.RedisService import RedisService
from services.spark import spark_predict
from utils import date_utils

logger = logger_factory.create_logger(__name__)


def process():
  short_model_id = "ICN7780367212284440071"  # fut_prof 68%
  start_date_str = "2019-07-17"
  end_date_str = "2019-08-15"
  num_slices = None

  package_folders = [
    "tsczuz__2019-08-14_20-25-02-514.76",
    "tsczuz__2019-08-13_07-44-49-19.99",
    "tsczuz__2019-08-13_08-02-50-934.15",
    "tsczuz__2019-08-13_23-05-25-314.13",
    "tsczuz__2019-08-13_08-19-29-765.36",
    "tsczuz__2019-08-13_19-33-09-981.39",
    "tsczuz__2019-08-13_19-50-20-163.87",
    "tsczuz__2019-08-14_20-12-46-174.24",
    "tsczuz__2019-08-13_20-15-02-219.27",
    "tsczuz__2019-08-13_20-32-05-80.24",
    "tsczuz__2019-08-13_21-05-59-524.71",
    "tsczuz__2019-08-13_21-16-52-909.21",
    "tsczuz__2019-08-14_20-54-17-876.95",
    "tsczuz__2019-08-14_22-01-55-582.07",
    "tsczuz__2019-08-14_22-27-57-370.67",
    "tsczuz__2019-08-14_22-41-04-602.12",  # 2018-09-07
    "tsczuz__2019-08-14_22-56-11-292.63",  # 2018-09-08
    "tsczuz__2019-08-14_23-08-01-480.4",  # 2018-09-09
    "tsczuz__2019-08-16_15-37-00-100.37",  # 2018-09-12
    "tsczuz__2019-08-16_16-05-16-615.71",  # 2018-09-13
    "tsczuz__2019-08-16_16-15-13-516.37",  # 2018-09-14
    "tsczuz__2019-08-16_16-27-21-122.39",  # 2018-09-15
  ]

  files = []
  for pck in package_folders:
    data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "test_one_day")
    image_dir = os.path.join(data_cache_dir, pck)

    files.extend(file_services.walk(image_dir))

  spark_arr = []
  for f in files:
    spark_arr.append({"file_path": f, "short_model_id": short_model_id})

  # spark_arr = spark_arr[0:300]

  logger.info(f"Found {len(spark_arr)} files to process.")

  results = do_spark(spark_arr, num_slices=num_slices)

  df = pd.DataFrame(results)
  df.sort_values(by=['date', 'symbol'], inplace=True)
  output_path = os.path.join(config.constants.CACHE_DIR, f"scores_{short_model_id}_{start_date_str}_{end_date_str}.csv")

  logger.info(f"\nAbout to write dataframe file {output_path} ...\n")
  df.to_csv(output_path, index=False)


def do_spark(spark_arr, num_slices=None):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.stop()

  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(spark_arr, num_slices)
  logger.info("About to invoke rdd map method ...")
  results = rdd.map(spark_process).collect()

  logger.info(f"Got {len(results)} results.")

  sc.stop()

  return results


def spark_process(thing_dict):
  file_path = thing_dict["file_path"]
  short_model_id = thing_dict['short_model_id']
  spark_file_path = SparkFiles.get(file_path)

  key_cache = spark_predict.get_prediction_cache_key(spark_file_path, short_model_id)

  redis_service = RedisService()
  record = redis_service.read_as_json(key_cache)

  df = EquityUtilService.get_df_from_ticker_path(record["symbol"], True)
  if df is not None:
    print(f"symbol: {record['symbol']}")

    yield_date_str = record['date']
    if yield_date_str is not None:
      bet_date = date_utils.parse_datestring(yield_date_str) - timedelta(days=1)
      df_yield_date = df[df['date'] == yield_date_str]

      record["high"] = df_yield_date['high'].values[0]
      record["low"] = df_yield_date['low'].values[0]
      record["close"] = df_yield_date['close'].values[0]

      df_bet_date = df[df['date'] == date_utils.get_standard_ymd_format(bet_date)]
      if df_bet_date.shape[0] > 0:
        record['bet_price'] = df_bet_date['close'].values[0]
      else:
        record['bet_price'] = None

  return record


if __name__ == "__main__":
  logger.info("Run from command line...")
  process()
