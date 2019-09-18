import os
import shutil
from datetime import timedelta
from threading import Thread
from unittest import TestCase

import config
from config.logger_factory import logger_factory
from services import eod_data_service, split_eod_data_to_files_service, file_services
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils

import pandas as pd
logger = logger_factory.create_logger(__name__)

class TestEquityUtilService(TestCase):

  def test_find_missing_days(self):
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    end_date = date_utils.parse_datestring("2019-06-14")
    until_date = end_date + timedelta(days=3)
    eod_data_service.find_and_download_missing_days(df, until_date)

  def test_merge(self):
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.LARGE)

    df_merged = eod_data_service.merge_shar_equity_price_data(df)

    assert(df_merged.shape[0] > df.shape[0])

  def test_get_todays_merged_shar_data(self):
    # Arrange
    df_merged = eod_data_service.get_todays_merged_shar_data()

    # Assert
    assert(df_merged.shape[0] > 0)

  def test_foo(self):
    rows = list()
    rows.append({"ticker": "foo", "date": "2019-01-01", "high": 1})
    rows.append({"ticker": "foo", "date": "2019-01-02", "high": 2})
    rows.append({"ticker": "foo", "date": "2019-01-03", "high": 3})
    rows.append({"ticker": "foo", "date": "2019-01-04", "high": 4})
    rows.append({"ticker": "foo", "date": "2019-01-05", "high": 5})

    rows.append({"ticker": "bar", "date": "2019-01-01", "high": 1})
    rows.append({"ticker": "bar", "date": "2019-01-02", "high": 2})
    rows.append({"ticker": "bar", "date": "2019-01-03", "high": 3})
    rows.append({"ticker": "bar", "date": "2019-01-04", "high": 4})
    rows.append({"ticker": "bar", "date": "2019-01-05", "high": 5})

    df = pd.DataFrame(rows)

    df = df.sample(frac=1).reset_index(drop=True)
    df_sorted = df.sort_values(by=['date'])

    df_grouped = df_sorted.groupby("ticker")

    acc = []
    def filter(df_ticker):
      # logger.info(df_ticker.head())
      assert(df_ticker.iloc[0,:]['high'] == 1)

      ndarray = df_ticker.to_records(index=True)
      high_values = ndarray['high'].tolist()
      logger.info(high_values)
      acc.append(high_values)

    df_grouped.filter(lambda x: filter(x))

    assert(len(acc) == 2)

  def test_spark_split_shar_equity_to_ticker_files(self):
    # Arrange
    # Act
    stock_infos = split_eod_data_to_files_service.split_shar_equity_to_ticker_files()

    # Assert
    assert(len(stock_infos) > 0)

  def test_get_tesla(self):
    # Arrange
    # Act
    df = EquityUtilService.get_df_from_ticker_path('tsla', False)

    # Assert
    assert(df.shape[0] > 1000)

  def test_select_single_day_equity_data(self):
    # Arrange
    yield_date = date_utils.parse_datestring('2019-08-16')
    trading_days_avail = 1000
    min_price = 5.0
    amount_to_spend = 25000

    # Act
    df = EquityUtilService.select_single_day_equity_data(yield_date, trading_days_avail=trading_days_avail, min_price=min_price, amount_to_spend=amount_to_spend)

    # Assert
    assert(df.shape[0] > 1000)

  def test_filter_symbol_high_variability(self):
    # Arrange
    expected_remained = 'c:/foo/1_ZYTO_2018-12-31.png'
    file_paths = [
      'c:/foo/0_ZUO_2018-12-31.png',
      expected_remained
    ]

    # Act
    result_list = EquityUtilService.filter_high_variability(file_paths, False)

    # Assert
    assert(len(result_list) == 1)
    assert(result_list[0] == expected_remained)


  def test_foo(self):
    folder_name = "process_2019-09-14_11-47-31-845.11"
    package_folder = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, 'selection_packages', "SelectChartZipUploadService", folder_name)
    train_test_dir = os.path.join(package_folder, "train_test")
    file_paths_false = file_services.walk(os.path.join(train_test_dir, "0"))
    file_paths_true = file_services.walk(os.path.join(train_test_dir, "1"))

    file_paths_false = file_paths_false[:len(file_paths_true)]

    file_paths = file_paths_false + file_paths_true

    destination_folder = os.path.join(package_folder, "train_test_equalized")
    for fp in file_paths:
      basename = os.path.basename(fp)
      parent_dirname = os.path.basename(os.path.dirname(fp))
      dest_parent = os.path.join(destination_folder, parent_dirname)
      os.makedirs(dest_parent, exist_ok=True)
      dest_path = os.path.join(dest_parent, basename)
      logger.info(f"Will write to {dest_path}.")

      Thread(target=shutil.copy, args=[fp, dest_path]).start()

    assert(len(file_paths) > 0)




