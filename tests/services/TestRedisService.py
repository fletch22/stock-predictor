import pickle
import zlib
from unittest import TestCase

import pandas as pd
from stopwatch import Stopwatch

import config
from config import logger_factory
from services import eod_data_service
from services.EquityUtilService import EquityUtilService
from services.RedisService import RedisService
from services.StockService import StockService

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

  def test_dataframe_time(self):
    # Arrange
    stopwatch = Stopwatch()
    stopwatch.start()
    df = eod_data_service.get_todays_merged_shar_data()
    stopwatch.stop()
    logger.info(f"Elapsed: {stopwatch}")

    redis_service = RedisService()
    key = "test"

    # Set
    stopwatch.reset()
    stopwatch.start()
    redis_service.write_df(key, df)
    stopwatch.stop()

    logger.info(f"Elapsed: {stopwatch}")

    stopwatch.reset()
    stopwatch.start()
    df = redis_service.read_df(key)
    stopwatch.stop()

    logger.info(f"Elapsed: {stopwatch}")


