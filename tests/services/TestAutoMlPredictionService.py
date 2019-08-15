import json
import os
import random
from datetime import timedelta
from unittest import TestCase

import findspark
from pyspark import SparkFiles, SparkContext
from stopwatch import Stopwatch

import config
from config import logger_factory
from services import file_services, spark_predict
from services.AutoMlPredictionService import AutoMlPredictionService
from services.EquityUtilService import EquityUtilService
from services.RedisService import RedisService
from utils import date_utils
import pandas as pd

logger = logger_factory.create_logger(__name__)

class TestAutoMlPredictionService(TestCase):

  def test_predict_and_calculate(self):
    # Arrange
    short_model_id = "ICN7780367212284440071" # fut_prof 68%
    # short_model_id = "ICN5723877521014662275" # closeunadj
    # package_folder = "process_2019-08-04_09-08-53-876.24"
    # package_folder = "process_2019-08-06_21-07-03-123.23"
    # package_folder = "process_2019-08-07_08-27-02-182.53"
    # package_folder = "process_2019-08-07_21-01-14-275.25"
    # package_folder = "process_2019-08-08_21-28-43-496.67"
    # data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
    # image_dir = os.path.join(data_cache_dir, "test_holdout")

    # package_folder = "tsczuz__2019-08-13_07-44-49-19.99"  # 2019-07-17 .56 -> -.36
    # package_folder = "tsczuz__2019-08-13_08-02-50-934.15"  # 2019-07-18 .56 -> -.07
    # package_folder = "tsczuz__2019-08-13_23-05-25-314.13"  # 2019-07-19 .56 -> .1
    # package_folder = "tsczuz__2019-08-13_08-19-29-765.36"  # 2019-07-22 .56 -> .14
    # package_folder = "tsczuz__2019-08-13_08-34-11-93.47"  # 2019-07-23 .56 -> .39
    # package_folder = "tsczuz__2019-08-13_19-33-09-981.39"  # 2019-07-24 .56 -> .48
    # package_folder = "tsczuz__2019-08-13_19-50-20-163.87"  # 2019-07-25 .56 -> -.09
    # package_folder = "tsczuz__2019-08-14_20-12-46-174.24"  # 2019-07-26 .56 -> .31
    # package_folder = "tsczuz__2019-08-13_20-15-02-219.27"  # 2019-07-29 .56 -> -.18
    # package_folder = "tsczuz__2019-08-13_20-32-05-80.24" #2019-07-30 .56 -> .33
    # package_folder = "tsczuz__2019-08-13_21-05-59-524.71"  # 2019-07-31 .56 -> .31
    # package_folder = "tsczuz__2019-08-13_21-16-52-909.21" # 2019-08-01 .56 -> .08
    # package_folder = "tsczuz__2019-08-14_20-54-17-876.95" # 2019-08-02 .56 -> -.11
    # package_folder = "tsczuz__2019-08-14_22-01-55-582.07"  # 2019-08-05 .56 -> -.31
    # package_folder = "tsczuz__2019-08-14_22-27-57-370.67"  # 2019-08-06 .56 -> .89
    # package_folder = "tsczuz__2019-08-14_22-41-04-602.12"  # 2019-08-07 .56 -> .03
    # package_folder = "tsczuz__2019-08-14_22-56-11-292.63"  # 2019-08-08 .56 -> .68
    package_folder = "tsczuz__2019-08-14_23-08-01-480.4"  # 2019-08-09 .56 -> -.71
    # package_folder = ""  # 2019-08-10 .56 -> .

    data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "test_one_day")
    image_dir = os.path.join(data_cache_dir, package_folder)
    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=.56)
    sought_gain_frac = .01

    # Act
    stopwatch = Stopwatch()
    stopwatch.start()
    auto_ml_service.predict_and_calculate(image_dir, sought_gain_frac, max_files=3000, purge_cached=False)
    stopwatch.stop()

    logger.info(f"Elapsed time: {round(stopwatch.duration/60, 2)} minutes")

    # Assert
    assert(True)

  def test_scores(self):
    # Arrange
    short_model_id = "ICN7780367212284440071"  # fut_prof 68%
    start_date_str = "2019-07-17"
    end_date_str = "2019-08-09"

    package_folders = [
      "tsczuz__2019-08-14_20-25-02-514.76",
      "tsczuz__2019-08-13_07-44-49-19.99",
      "tsczuz__2019-08-13_08-02-50-934.15",
      "tsczuz__2019-08-13_23-05-25-314.13",
      "tsczuz__2019-08-13_08-19-29-765.36",
      "tsczuz__2019-08-13_08-34-11-93.47",
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
      "tsczuz__2019-08-14_22-41-04-602.12",
      "tsczuz__2019-08-14_22-56-11-292.63",
      "tsczuz__2019-08-14_23-08-01-480.4"
    ]

    files = []
    for pck in package_folders:
      data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "test_one_day")
      image_dir = os.path.join(data_cache_dir, pck)

      files.extend(file_services.walk(image_dir))

    spark_arr = []
    for f in files:
      spark_arr.append({"file_path": f, "short_model_id": short_model_id})

    results = do_spark(spark_arr, num_slices=None)

    df = pd.DataFrame(results)
    df.sort_values(by=['date', 'symbol'], inplace=True)
    output_path = os.path.join(config.constants.CACHE_DIR, f"scores_{short_model_id}_{start_date_str}_{end_date_str}.csv")

    df.to_csv(output_path, index=False)

    logger.info(f"Results: {results}")


def do_spark(spark_arr, num_slices=None):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(spark_arr, num_slices)
  results = rdd.map(process).collect()

  sc.stop()

  return results

def process(thing_dict):
  file_path = thing_dict["file_path"]
  short_model_id = thing_dict['short_model_id']
  spark_file_path = SparkFiles.get(file_path)

  key_cache = spark_predict.get_prediction_cache_key(spark_file_path, short_model_id)

  redis_service = RedisService()
  record = redis_service.read_as_json(key_cache)

  logger.info(f"symbol: {record['symbol']}")
  df = EquityUtilService.get_df_from_ticker_path(record["symbol"], True)
  if df is not None:

    yield_date_str = record['date']
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
