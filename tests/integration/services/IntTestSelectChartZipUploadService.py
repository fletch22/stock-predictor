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
from services.SparkRenderImages import SparkRenderImages
from services.StockService import StockService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestSelectChartZipUploadService(TestCase):

  def test_create_daily_set(self):
    # Arrange
    yield_date = date_utils.parse_datestring("2019-07-18")
    pct_gain_sought = 1.0
    num_days_to_sample = 1000
    max_symbols = 100
    min_price = 5.0
    amount_to_spend = 25000
    chart_type = ChartType.Neopolitan
    volatility_min = 2.80

    df, package_path, image_dir = SparkRenderImages.render_train_test_day(yield_date, pct_gain_sought, num_days_to_sample, max_symbols=max_symbols, min_price=min_price,
                                                                          amount_to_spend=amount_to_spend, chart_type=chart_type, volatility_min=volatility_min)

    # Assert
    num_files = len(file_services.walk(image_dir))
    num_records = len(df["ticker"].unique().tolist())

    assert(num_files == num_records)
