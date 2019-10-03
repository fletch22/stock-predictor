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
from services.quality_verifiers.verify_folder_image_quality import get_info_from_file_path
from services.spark import spark_predict
from services.spark.spark_predict import get_prediction_from_automl, get_prediction_cache_key
from utils import date_utils

logger = logger_factory.create_logger(__name__)


def process():
  # short_model_id = "ICN7780367212284440071"  # fut_prof 68%
  # short_model_id = "ICN2174544806954869914" # multi-2019-09-01 66%
  short_model_id = "ICN2383257928398147071"  # vol_eq_09_14_11_47_31_845_11 65%
  start_date_str = "2019-07-17"
  end_date_str = "2019-09-06"
  num_slices = 6

  # package_folders = [
  #   "process_2019-09-08_00-34-23-915.84",
  #   "process_2019-09-08_00-53-16-893.42",
  #   "process_2019-09-08_01-10-29-697.36",
  #   "process_2019-09-08_01-27-57-107.17",
  #   "process_2019-09-08_01-45-04-772.09",
  #   "process_2019-09-08_02-01-54-191.66",
  #   "process_2019-09-08_02-19-11-203.06",
  #   "process_2019-09-08_02-36-23-407.33",
  #   "process_2019-09-08_02-53-29-30.1",
  #   "process_2019-09-08_03-10-21-60.37",
  #   "process_2019-09-08_03-25-24-88.83",
  #   "process_2019-09-08_03-40-46-933.23",
  #   "process_2019-09-08_03-57-18-727.55",
  #   "process_2019-09-08_04-12-36-319.12",
  #   "process_2019-09-08_04-28-50-959.87",
  #   "process_2019-09-08_04-45-11-175.0",
  #   "process_2019-09-08_05-01-37-295.06",
  #   "process_2019-09-08_05-18-44-210.43",
  #   "process_2019-09-08_05-35-43-847.89",
  #   "process_2019-09-08_05-52-37-590.89",
  #   "process_2019-09-08_06-08-31-603.66",
  #   "process_2019-09-08_06-25-28-697.92",
  #   "process_2019-09-08_06-43-02-656.38",
  #   "process_2019-09-08_07-00-43-174.04",
  #   "process_2019-09-08_07-18-22-602.64",
  #   "process_2019-09-08_07-35-59-441.77",
  #   "process_2019-09-08_07-52-07-346.76",
  #   "process_2019-09-08_08-09-31-651.05",
  #   "process_2019-09-08_08-26-08-332.79",
  #   "process_2019-09-08_08-43-32-357.49",
  #   "process_2019-09-08_09-01-04-692.85",
  #   "process_2019-09-08_09-18-51-2.44"
  # ]

  package_folders = [
    "process_2019-09-15_08-43-07-937.8",
    "process_2019-09-15_09-27-51-886.62",
    "process_2019-09-15_09-38-35-121.39",
    "process_2019-09-15_09-51-39-65.42",
    "process_2019-09-15_10-03-59-580.86",
    "process_2019-09-15_12-55-54-861.15",
    "process_2019-09-15_13-01-04-792.3",
    "process_2019-09-15_13-05-26-527.33",
    "process_2019-09-15_13-31-26-262.48",
    "process_2019-09-15_13-35-43-474.99",
    "process_2019-09-15_13-39-32-870.98",
    "process_2019-09-15_13-43-25-959.34",
    "process_2019-09-15_13-45-52-578.17",
    "process_2019-09-15_13-50-10-391.36",
    "process_2019-09-15_13-54-19-578.46",
    "process_2019-09-15_13-58-15-340.5",
    "process_2019-09-15_14-02-07-600.9",
    "process_2019-09-15_15-56-00-356.66",
    "process_2019-09-15_16-00-11-48.41",
    "process_2019-09-15_16-04-00-588.58",
    "process_2019-09-15_16-07-42-827.66",
    "process_2019-09-15_16-11-40-753.51",
    "process_2019-09-15_16-15-23-25.45",
    "process_2019-09-15_16-19-05-587.39",
    "process_2019-09-15_16-23-03-394.13",
    "process_2019-09-15_16-26-47-851.12",
    "process_2019-09-15_16-30-25-885.47",
    "process_2019-09-15_16-34-06-17.38",
    "process_2019-09-15_16-37-38-928.1",
    "process_2019-09-15_16-41-08-259.13",
    "process_2019-09-15_16-44-44-97.67",
    "process_2019-09-15_16-48-14-245.38",
    "process_2019-09-15_16-51-55-456.24",
    "process_2019-09-15_16-55-31-225.99",
    "process_2019-09-15_16-59-07-574.45",
    "process_2019-09-15_17-03-04-8.06",
    "process_2019-09-15_17-06-36-672.94",
    "process_2019-09-15_17-10-30-37.45",
    "process_2019-09-15_17-14-26-988.57",
    "process_2019-09-15_17-17-49-828.36",
    "process_2019-09-15_17-18-25-649.01",
    "process_2019-09-15_17-22-11-34.78",
    "process_2019-09-15_17-22-46-881.79",
    "process_2019-09-15_17-23-22-805.8"
  ]

  purge_cache = False
  files = []
  data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService")
  for pck in package_folders:
    image_dir = os.path.join(data_cache_dir, pck, 'graphed')
    files.extend(file_services.walk(image_dir))

    logger.info(f"Looking in {image_dir}")

  # image_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", "vol_eq")
  # folder_dirs = file_services.os.listdir(image_dir)
  #
  # basename_list = [os.path.basename(f) for f in folder_dirs]
  #
  # for name in basename_list:
  #   print(f"\"{name}\", ")

  spark_arr = []
  for f in files:
    spark_arr.append({"file_path": f, "short_model_id": short_model_id, "purge_cache": purge_cache})

  logger.info(f"Found {len(spark_arr)} records to process.")

  results = do_spark(spark_arr, num_slices=num_slices)

  logger.info(f"Results {len(results)}")

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
  image_path = thing_dict["file_path"]
  short_model_id = thing_dict['short_model_id']
  purge_cache = thing_dict['purge_cache']

  image_path = SparkFiles.get(image_path)

  logger.info(f"Collecting data for {short_model_id}; {image_path}")

  key_pred = get_prediction_cache_key(image_path, short_model_id)
  redis_service = RedisService()

  prediction = None
  if not purge_cache:
    prediction = redis_service.read_as_json(key_pred)

  if prediction is None:
    category_actual, symbol, date_str = get_info_from_file_path(image_path)
    prediction = get_prediction_from_automl(symbol, category_actual, image_path, short_model_id, .50)
    redis_service.write_as_json(key_pred, prediction)
  else:
    logger.info("Found prediction in Redis cache.")

  redis_service.close_client_connection()

  df = EquityUtilService.get_df_from_ticker_path(prediction["symbol"], True)
  if df is not None:
    logger.debug(f"symbol: {prediction['symbol']}")

    yield_date_str = prediction['date']
    if yield_date_str is not None:
      bet_date = date_utils.parse_std_datestring(yield_date_str) - timedelta(days=1)
      df_yield_date = df[df['date'] == yield_date_str]

      prediction["high"] = df_yield_date['high'].values[0]
      prediction["low"] = df_yield_date['low'].values[0]
      prediction["close"] = df_yield_date['close'].values[0]

      df_bet_date = df[df['date'] == date_utils.get_standard_ymd_format(bet_date)]
      if df_bet_date.shape[0] > 0:
        prediction['bet_price'] = df_bet_date['close'].values[0]
      else:
        prediction['bet_price'] = None

  return prediction


if __name__ == "__main__":
  logger.info("Run from command line...")
  process()
