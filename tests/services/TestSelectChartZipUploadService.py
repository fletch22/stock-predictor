import os
import shutil
from unittest import TestCase

from datetime import datetime, timedelta
import numpy as np

import findspark
from pyspark import SparkContext, SparkFiles
import pandas as pd
import config
from config import logger_factory
from services import file_services, chart_service, eod_data_service
from services.CloudFileService import CloudFileService
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.SelectChartZipUploadService import SelectChartZipUploadService
from services.SparkRenderImages import SparkRenderImages
from services.StockService import StockService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestSelectChartZipUploadService(TestCase):
  files_to_delete = []
  cloud_files_to_delete = []
  cloud_file_service = CloudFileService()

  def tearDown(self):
    for d in self.files_to_delete:
      shutil.rmtree(d)

    for f in self.cloud_files_to_delete:
      self.cloud_file_service.delete_file_from_uploads(f)

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
    SelectChartZipUploadService.prep_for_upload(prediction_dir, 4)

    # Assert
    file_0_0 = os.path.join(prediction_dir, "test_holdout", "0", "0.txt")
    file_1_1 = os.path.join(prediction_dir, "test_holdout", "1", "1.txt")
    assert(os.path.exists(file_0_0))
    assert(os.path.exists(file_1_1))

    train_test_file_0_0 = os.path.join(prediction_dir, "train_test", "0", "0.txt")
    assert (os.path.exists(train_test_file_0_0))

  def test_target_date(self):
    df = eod_data_service.get_todays_merged_shar_data()

  def test_create_daily_set(self):
    # Arrange
    yield_date = date_utils.parse_datestring("2019-07-18")
    pct_gain_sought = 1.0
    num_days_to_sample = 1000

    df, image_dir = SparkRenderImages.render_train_test_day(yield_date, pct_gain_sought, num_days_to_sample)

    # Assert
    num_files = len(file_services.walk(image_dir))
    num_records = len(df["ticker"].unique().tolist())

    assert(num_files == num_records)

  def test_create_learning_set(self):
    min_price = 5.0
    amount_to_spend = 25000
    trading_days_span = 1000
    min_samples = 120000
    pct_gain_sought = 1.0
    start_date: datetime = None  # date_utils.parse_datestring("2015-07-23")
    end_date: datetime = date_utils.parse_datestring("2019-07-16")
    pct_test_holdout = 10

    test_train_dir = SelectChartZipUploadService.create_learning_set(start_date, end_date, min_samples, pct_gain_sought, trading_days_span, pct_test_holdout, min_price, amount_to_spend)

    logger.info(f"Train/Test Dir: {test_train_dir}")

  def test_foo(self):
    # df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    file_path = "C:\\Users\\Chris\\workspaces\\data\\financial\\quandl\\tables\\splits\\A.csv"
    df = pd.read_csv(file_path)

    yield_date = date_utils.parse_datestring("2019-08-09")

    # df = df[df['date'] <= date_utils.get_standard_ymd_format(yield_date)]
    df.sort_values(by=['date'], inplace=True)

    df_tail = df.tail(5)

    logger.info(df_tail['date'].values[:-1])
