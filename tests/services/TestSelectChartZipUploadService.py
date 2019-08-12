import os
import shutil
from unittest import TestCase

from datetime import datetime, timedelta

import findspark
from pyspark import SparkContext, SparkFiles

import config
from config import logger_factory
from services import file_services
from services.CloudFileService import CloudFileService
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.SelectChartZipUploadService import SelectChartZipUploadService
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

  # def test_select_chart_zip_upload(self):
  #   # prediction_dir = os.path.join(config.constants.CACHE_DIR, "prediction_test")
  #   # test_dir = SelectAndChartService.prep_for_upload(prediction_dir, 4)
  #
  #   # Arrange
  #   trading_days_span = 3
  #
  #   # Act
  #   output_dir, cloud_dest_path = SelectChartZipUploadService.process(50, trading_days_span=trading_days_span, hide_image_details=False, sample_file_size=SampleFileTypeSize.SMALL)
  #   self.cloud_files_to_delete.append(cloud_dest_path)
  #
  #   logger.info(f"Output dir: {output_dir}")
  #
  #   # Assert
  #   assert(os.path.exists(output_dir))
  #   assert(self.cloud_file_service.file_exists_in_uploads(cloud_dest_path))

  def test_create_learning_set(self):
    SelectChartZipUploadService.create_learning_set()


  def test_with_data_ending_after_date(self):
    df = SelectChartZipUploadService.create_learning_set()

    # Create images

    assert(df.shape[0])

  def test_target_date(self):
    df = EquityUtilService.get_todays_merged_shar_data()



  def test_create_daily_set(self):
    # Arrange
    amount_to_spend = 25000
    num_days_available = 1000
    min_price = 5.0

    yield_date = date_utils.parse_datestring("2019-07-17")

    # get merged data
    df = EquityUtilService.get_todays_merged_shar_data()

    df_min_filtered = StockService.filter_equity_basic_criterium(amount_to_spend, num_days_available, min_price, df)

    symbols = df_min_filtered["ticker"].unique().tolist()

    symbols_package = []
    for s in symbols:
      symb_pck = {
        "symbol": s,
        "yield_date": yield_date
      }
      symbols_package.append(symb_pck)

    # Call
    symbols_package = symbols_package[0:3]
    do_spark(symbols_package)

    # symbols_stuff_list = []
    # def filter(df_symbol):
    #   df_symbol.sort_values(by=['date'], inplace=False)
    #
    #   symbol = df_symbol.iloc[0, :]['ticker']
    #
    #   symbols_stuff = {
    #     "symbol": symbol,
    #
    #   }
      # bet_date = date_utils.parse_datestring(df_symbol.iloc[-2, :]['date'])
      # bet_price = df_symbol.iloc[-2, :]['close']
      # yield_date = date_utils.parse_datestring(df_symbol.iloc[-1, :]['date'])
      # high = df_symbol.iloc[-1, :]['high']
      # low = df_symbol.iloc[-1, :]['low']
      # close = df_symbol.iloc[-1, :]['low']
      #
    #   ndarray = df_sorted.to_records(index=True)
    #   high_values = ndarray['high'].tolist()
    #
    #   sinfo = {
    #     "symbol": symbol,
    #     "values": high_values
    #   }
    #   stock_infos.append(sinfo)
    # df.groupby('ticker').filter(lambda x: filter(x))

    # fetch all today's price data.
    # Update ticker files.
    # get data using today as last day in survey
    # draw image
    # Submit each image to automl


def do_spark(spark_arr):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(spark_arr)
  rdd.foreach(process)

  sc.stop()

def process(symbols):
  import pandas as pd
  from config import logger_factory

  logger = logger_factory.create_logger(__name__)

  for s in symbols:
    logger.info(f"Processing {s}")
    split_file_path = os.path.join(config.constants.SHAR_SPLIT_EQUITY_PRICES_DIR, f"{s}.csv")
    file_path_spark = SparkFiles.get(split_file_path)
    df = pd.read_csv(file_path_spark)
