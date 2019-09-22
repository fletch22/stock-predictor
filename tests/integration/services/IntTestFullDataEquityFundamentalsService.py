import os
import statistics
from datetime import datetime
from unittest import TestCase

from stopwatch import Stopwatch

import config
from config import logger_factory
from services import eod_data_service
from services.equities.FullDataEquityFundamentalsService import FullDataEquityFundamentalsService
from utils import date_utils
import pandas as pd
import numpy as np

logger = logger_factory.create_logger(__name__)

class TestFullDataEquityFundamentalsService(TestCase):

  def test_open_fundamentals(self):
    # Arrange
    efs = FullDataEquityFundamentalsService()

    # Act
    df = efs.df

    logger.info(f"Cols: {df.columns}")

    assert (df.shape[0] > 0)

  def test_get_fundies(self):
    # Arrange
    stopwatch = Stopwatch()
    efs = FullDataEquityFundamentalsService()

    efs.filter(['ibm', 'aapl', 'msft'])

    end_date = date_utils.parse_datestring('2019-01-23')

    # Act
    stopwatch.start()
    fundies = efs.get_fundamentals_at_point_in_time('googl', end_date, ['pe'])
    stopwatch.stop()

    logger.info(f"Fundies: {fundies}; elapsed: {stopwatch}")



