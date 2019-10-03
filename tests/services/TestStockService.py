from unittest import TestCase

from config.logger_factory import logger_factory
from services import eod_data_service
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class TestStockService(TestCase):

  def test_filter_by_recent_symbols_no_data_for_date(self):
      # Arrange
      df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

      expected_date = '2019-06-15'
      target_date = date_utils.parse_std_datestring(expected_date)

      # Act
      df_filtered, date_str_used = StockService.filter_by_recent_symbols(df, target_date=target_date, get_most_recent_prev_date=False)

      # Assert
      assert(df_filtered.shape[0] == 0)
      assert(date_str_used == expected_date)

  def test_filter_by_recent_symbols_day_after_last_day(self):
    # Arrange
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    target_date = date_utils.parse_std_datestring('2019-06-15')

    # Act
    df_filtered, date_str_used = StockService.filter_by_recent_symbols(df, target_date=target_date, get_most_recent_prev_date=True)

    # Assert
    assert (df_filtered.shape[0] > 0)
    assert(date_str_used == '2019-06-14')


  def test_filter_by_recent_symbols_last_day(self):
    # Arrange
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    date_str_expected = '2019-06-14'
    target_date = date_utils.parse_std_datestring(date_str_expected)

    # Act
    df_filtered, date_str_actual = StockService.filter_by_recent_symbols(df, target_date=target_date, get_most_recent_prev_date=False)

    # Assert
    assert (df_filtered.shape[0] > 0)
    assert(date_str_actual == date_str_expected)

  def test_filter_by_recent_symbols_4_days_after_last(self):
    # Arrange
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    date_str_expected = '2019-06-23'
    target_date = date_utils.parse_std_datestring(date_str_expected)

    # Act
    df_filtered, date_str_actual = StockService.filter_by_recent_symbols(df, target_date=target_date, get_most_recent_prev_date=True)

    # Assert
    assert (df_filtered.shape[0] > 0)
    assert(date_str_actual == '2019-06-14')
  