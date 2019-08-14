import time

import findspark
from google.cloud import automl
from pyspark import SparkFiles, SparkContext

import config
from config import logger_factory
from services import file_services
from services.RedisService import RedisService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

def get_spark_results(image_info):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.stop()

  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(image_info, numSlices=4)

  results = rdd.map(spark_category_predict).collect()

  sc.stop()

  return results

def spark_category_predict(image_info):
  symbol = image_info["symbol"]
  image_path = SparkFiles.get(image_info["file_path"])
  short_model_id = image_info["short_model_id"]
  category_actual = image_info["category_actual"]
  score_threshold = image_info["score_threshold"]
  purge_cached = image_info["purged_cached"]

  key_pred = get_prediction_cache_key(image_path, short_model_id)
  redis_service = RedisService()

  prediction = None
  if not purge_cached:
    prediction = redis_service.read_as_json(key_pred)

  if prediction is None:
    prediction = get_prediction_from_automl(symbol, category_actual, image_path, short_model_id, score_threshold)
    redis_service.write_as_json(key_pred, prediction)
  else:
    logger.info("Found prediction in Redis cache.")

  redis_service.close_client_connection()

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

  row = {
    "symbol": symbol,
    "image_path": image_path,
    "date": None,
    "category_actual": category_actual,
    "category_predicted": None,
    "score": None,
    "model_full_id": model_full_id
  }

  try:
    response = automl_client.predict(model_full_id, payload, params)
  except BaseException:
    logger.info(f"ERROR!!! Problem with symbol {symbol} at {image_path}.")
    return row
    # pause_secs = 60
    # logger.info(f"Got AutoMl predict exception. Guessing it was 'Gateway Timeout'. Pausing {pause_secs} seconds then trying once more.")
    # time.sleep(pause_secs)
    # automl_client, model_full_id = get_automl_client(short_model_id)
    # response = automl_client.predict(model_full_id, payload, params)

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