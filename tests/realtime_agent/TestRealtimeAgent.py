import random
from datetime import datetime, timedelta
from statistics import mean
from unittest import TestCase

import matplotlib.pyplot as plt

from config import logger_factory
from realtime_agent import realtime_agent_model_runner, chart_realtime_agent_results
from services import eod_data_service
from services.StockService import StockService
from utils import date_utils
import tensorflow as tf
logger = logger_factory.create_logger(__name__)


class TestRealtimeAgent(TestCase):

  def test_gpu(self):
    sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))

  def test_model(self):
    agg_gains = []
    desired_trading_days = 253 * 2

    num_symb_desired = 200
    latest_date = date_utils.parse_datestring("2019-05-23")

    symbol = "googl"
    # symbols = [symbol]
    symbols = StockService.get_random_symbols_with_date(num_symb_desired, desired_trading_days=desired_trading_days, end_date=latest_date)

    for s in symbols:
      df_close, states_buy, states_sell, roi_amount, roi_pct = realtime_agent_model_runner.run(s, desired_trading_days, latest_date)
      agg_gains.append(roi_pct)
      logger.info(f"Symbol {s} roi: {round(roi_pct, 2)} ")

      mean_gains = mean(agg_gains)
      logger.info(f"Mean gains: {round(mean_gains, 2)}%")

    mean_gains = mean(agg_gains)
    logger.info(f"Mean gains: {round(mean_gains, 2)}%")


  def test_get_random_symbols(self):
    # Arrange
    num_desired = 10
    end_date = date_utils.parse_datestring("2019-05-23")
    desired_trading_days = 253 * 2

    symbols = StockService.get_random_symbols_with_date(num_desired, desired_trading_days=desired_trading_days, end_date=end_date)

    # Act
    logger.info(f"Symbols: {symbols}")





