import os
from unittest import TestCase

import config
from config import logger_factory
from services import chart_service
import pandas as pd

logger = logger_factory.create_logger(__name__)

class TestChartServices(TestCase):

  def test_chart_drawing(self):
    # Arrange
    expected_output_path = os.path.join(config.constants.CACHE_DIR, "1_IBM_2019-06-14.png")
    os.remove(expected_output_path)
    file_path = os.path.join(config.constants.QUANDL_DIR, "ibm_sample_1000.csv")

    df = pd.read_csv(file_path)

    save_dir = os.path.join(config.constants.CACHE_DIR)

    # Act
    chart_service.plot_and_save_for_learning(df, save_dir, "1")

    # Assert
    assert(os.path.exists(expected_output_path))

  # def test_