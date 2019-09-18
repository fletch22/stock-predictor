import pickle
import zlib
from unittest import TestCase

import pandas as pd
from stopwatch import Stopwatch

import config
from config import logger_factory
from services.RedisService import RedisService
from services.spark import spark_predict

logger = logger_factory.create_logger(__name__)

class TestRedisService(TestCase):

  def test_connection(self):
    redis_service = RedisService()

  def test_write_string(self):
    # Arrange
    redis_service = RedisService()

    # Act
    redis_service.write_string("foo", "bar")
    redis_service.write_as_json("foo_list", [1, 2, 3])

    # Assert
    str_value = redis_service.read("foo")
    assert(str_value == "bar")

    obj_value = redis_service.read_as_json("foo_list")
    assert (len(obj_value) == 3)

  def test_write_read_large_object(self):
    # Arrange
    stopwatch = Stopwatch()
    stopwatch.start()
    df = pd.read_csv(config.constants.SHAR_EQUITY_PRICES_SHORT)
    stopwatch.stop()
    logger.info(f"Read csv duration: {stopwatch.duration}")
    stopwatch.reset()

    key = "config.constants.SHAR_EQUITY_PRICES_SHORT"
    redis_service = RedisService()

    # Act
    stopwatch.start()
    redis_service.write_df(key, df)
    stopwatch.stop()

    duration_write = stopwatch.duration

    stopwatch.reset()
    stopwatch.start()
    redis_service.read_df(key)
    stopwatch.stop()

    duration_read = stopwatch.duration

    # Assert
    assert(duration_write < 125)
    assert(duration_read < 125)
    logger.info(f"Duration write:read - {duration_write}: {duration_read}")

  def test_foo(self):
    redis_service = RedisService()
    short_model_id = "ICN2174544806954869914"
    image_path = "C:\\Users\\Chris\\workspaces\\data\\financial\\output\\stock_predictor\\selection_packages\\SelectChartZipUploadService\\process_2019-09-05_19-18-00-268.24\\graphed\\1_CHMG_2019-07-18.png"

    key_pred = spark_predict.get_prediction_cache_key(image_path, short_model_id)

    thing = redis_service.read(key_pred)

    print(thing)
