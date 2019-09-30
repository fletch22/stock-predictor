import os
import shutil
from unittest import TestCase

from datetime import datetime, timedelta
import numpy as np

import findspark
from pyspark import SparkContext, SparkFiles
import pandas as pd
import config
from charts.ChartType import ChartType
from config import logger_factory
from services import file_services, chart_service, eod_data_service
from services.CloudFileService import CloudFileService
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.SelectChartZipUploadService import SelectChartZipUploadService
from services.StockService import StockService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestSelectChartZipUploadService(TestCase):

  def test_pred_files(self):
    # Arrange
    prediction_dir = file_services.create_unique_folder(config.constants.CACHE_DIR, "unit_test")
    file_list = [
      "0_ABC_test.txt",
      "0_DEF_test.txt",
      "0_GHI_test.txt",
      "1_DEF_test.txt",
      "1_GHI_test.txt",
      "1_JKL_test.txt",
    ]
    graphed_dir = os.path.join(prediction_dir, "graphed")
    os.makedirs(graphed_dir)

    for f in file_list:
      file_path = os.path.join(graphed_dir, f)
      with open(file_path, "w+") as f:
        f.write("This is a test.")

    # Act
    SelectChartZipUploadService.split_files_and_prep(100, prediction_dir, 4)

    # Assert
    file_0_0 = os.path.join(prediction_dir, "test_holdout", "0", "0.txt")
    file_1_1 = os.path.join(prediction_dir, "test_holdout", "1", "1.txt")
    assert(os.path.exists(file_0_0))
    assert(os.path.exists(file_1_1))

    train_test_file_0_0 = os.path.join(prediction_dir, "train_test", "0", "0.txt")
    assert (os.path.exists(train_test_file_0_0))
