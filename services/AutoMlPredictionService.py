import json
import os
import random
from statistics import mean

import findspark
import pandas as pd
from google.cloud import automl_v1beta1 as automl
from matplotlib import image
from pyspark import SparkContext, SparkFiles

import config
from config import logger_factory
from services import file_services
from services.EquityUtilService import EquityUtilService
from services.RedisService import RedisService
from services.StockService import StockService
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class AutoMlPredictionService():
  client = None
  package_dir = None
  short_model_id = None

  def __init__(self, short_model_id: str, package_dir: str, score_threshold: float = 0.5):
    self.client = automl.PredictionServiceClient.from_service_account_file(config.constants.CREDENTIALS_PATH)

    self.score_threshold = score_threshold
    self.package_dir = package_dir
    self.short_model_id = short_model_id

  def predict_and_calculate(self, image_dir: str, max_files=-1):
    file_paths = file_services.walk(image_dir)
    random.shuffle(file_paths, random.random)

    if max_files > -1 and len(file_paths) > max_files:
      file_paths = file_paths[:max_files]

    # Act
    image_info = []
    for f in file_paths:
      basename = os.path.basename(f)
      basename_parts = basename.split("_")
      category_actual = basename_parts[0]
      symbol = basename_parts[1]

      image_info.append({"category_actual": category_actual,
                         "symbol": symbol,
                         "file_path": f,
                         "score_threshold": self.score_threshold,
                         "short_model_id": self.short_model_id
                         })

    results = get_spark_results(image_info)

    logger.info(f"Results: {len(results)}")

    unique_symbols = set()
    for r in results:
      unique_symbols.add(r["symbol"])
    logger.info(f"Unique symbols: {len(unique_symbols)}")

    results_filtered_1 = [d for d in results if d['category_predicted'] == "1"]
    logger.info(f"Found category_predicted = 1: {len(results_filtered_1)}.")

    count = 0
    logger.info("About to get shar equity data for comparison calcs.")
    aggregate_gain = []
    sought_gain_pct = .01

    for r in results_filtered_1:
      eod_info = StockService.get_eod_of_date(r["symbol"], date_utils.parse_datestring(r["date"]))
      bet_price = eod_info["bet_price"]
      high = eod_info["high"]
      close = eod_info["close"]
      low = eod_info["low"]
      score = r["score"]
      category_actual = r["category_actual"]
      category_predicted = r["category_predicted"]
      symbol = r["symbol"]
      yield_date = r["date"]

      logger.info(f"Score: {score}; score_threshold: {self.score_threshold}")

      # if price_filter_max > bet_price > price_filter_min:
      if score > self.score_threshold:
        if category_actual == category_predicted:
          logger.info(f'Getting: {symbol}; {yield_date}')

          sought_gain = bet_price + (bet_price * sought_gain_pct)

          if high > sought_gain:
            aggregate_gain.append(sought_gain_pct)
          else:
            gain = (close - bet_price) / bet_price
            aggregate_gain.append(gain)

          count += 1
        else:
          loss = -1 * (bet_price - close) / bet_price
          aggregate_gain.append(loss)

    for n in aggregate_gain:
      logger.info(n)

    total_samples = len(aggregate_gain)
    logger.info(f"Back Test Accuracy: {count / total_samples}; total samples: {total_samples}; average gain: {mean(aggregate_gain)}")

def get_spark_results(image_info):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(image_info, numSlices=6)

  results = rdd.map(spark_category_predict).collect()

  sc.stop()

  return results

def spark_category_predict(image_info):
  symbol = image_info["symbol"]
  image_path = SparkFiles.get(image_info["file_path"])
  short_model_id = image_info["short_model_id"]
  category_actual = image_info["category_actual"]
  score_threshold = image_info["score_threshold"]

  key_pred = get_prediction_cache_key(image_path, short_model_id)
  redis_service = RedisService()
  prediction = redis_service.read_as_json(key_pred)

  if prediction is None:
    prediction = get_prediction_from_automl(symbol, category_actual, image_path, short_model_id, score_threshold)
    redis_service.write_as_json(key_pred, prediction)
  else:
    logger.info("Found prediction in Redis cache.")

  return prediction

def get_prediction_cache_key(image_path:str, model_full_id: str):
  return f"{model_full_id}_{image_path}"

def get_prediction_from_automl(symbol: str, category_actual: str, image_path: str, short_model_id: str, score_threshold: float):
  automl_client, model_full_id = get_automl_client(short_model_id)

  with open(image_path, "rb") as image_file:
    content = image_file.read()
  payload = {"image": {"image_bytes": content}}

  # params is additional domain-specific parameters.
  # score_threshold is used to filter the result
  # Initialize params
  params = {}
  if score_threshold:
    params = {"score_threshold": str(0.5)}

  response = automl_client.predict(model_full_id, payload, params)

  row = {
    "symbol": symbol,
    "image_path": image_path,
    "date": None,
    "category_actual": category_actual,
    "category_predicted": None,
    "score": None,
    "model_full_id": model_full_id
  }

  file_info = file_services.get_filename_info(image_path)
  if len(response.payload) == 0:
    logger.info("Got zero results.")
  else:
    for ndx, response_payload in enumerate(response.payload):
      category_predicted = response_payload.display_name
      score = response_payload.classification.score
      row["date"] = date_utils.get_standard_ymd_format(file_info["date"])
      row["category_predicted"] = category_predicted
      row["score"] = score
      logger.info(f"AutoML: {category_actual}; {category_predicted}; score:{round(score, 2)}")

  return row

def get_automl_client(short_model_id: str):
  client = automl.PredictionServiceClient.from_service_account_file(config.constants.CREDENTIALS_PATH)

  model_full_id = get_model_full_id(client, short_model_id)

  return client, model_full_id

def get_model_full_id(client, short_model_id):
  # Get the full path of the model.
  project_id = "fletch22-ai"
  compute_region = "us-central1"

  model_full_id = client.model_path(
    project_id, compute_region, short_model_id
  )

  return model_full_id