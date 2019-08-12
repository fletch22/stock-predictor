from datetime import timedelta
from unittest import TestCase

from config.logger_factory import logger_factory
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils

import pandas as pd
logger = logger_factory.create_logger(__name__)

class TestEquityUtilService(TestCase):

  def test_find_missing_days(self):
    df = EquityUtilService.get_shar_equity_data(SampleFileTypeSize.SMALL)

    end_date = date_utils.parse_datestring("2019-06-14")
    until_date = end_date + timedelta(days=3)
    EquityUtilService.find_and_download_missing_days(df, until_date)

  def test_merge(self):
    df = EquityUtilService.get_shar_equity_data(SampleFileTypeSize.LARGE)

    df_merged = EquityUtilService.merge_shar_equity_price_data(df)

    assert(df_merged.shape[0] > df.shape[0])

  def test_get_todays_merged_shar_data(self):
    # Arrange
    df_merged = EquityUtilService.get_todays_merged_shar_data()

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
    stock_infos = EquityUtilService.split_shar_equity_to_ticker_files()

    # Assert
    assert(len(stock_infos) > 0)






