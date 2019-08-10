from unittest import TestCase

from config import logger_factory
from services import stock_download_service
from utils import date_utils
import pandas as pd

logger = logger_factory.create_logger(__name__)

class TestStockDownloadService(TestCase):

  def test_download(self):
    # Arrange
    expected_start_date_str = "2019-07-18"
    expected_end_date_str = "2019-07-20"
    start_date = date_utils.parse_datestring(expected_start_date_str)
    end_date = date_utils.parse_datestring(expected_end_date_str)

    # Act
    destination_path = stock_download_service.download_supplemental(start_date, end_date)

    # Assert
    date_column_name = "date"
    df = pd.read_csv(destination_path)
    df_sorted = df.sort_values(by=[date_column_name], inplace=False)

    earliest_date_str = df_sorted.iloc[0,:][date_column_name]
    latest_date_str = df_sorted.iloc[-1,:][date_column_name]

    assert(earliest_date_str == expected_start_date_str)
    assert(latest_date_str == expected_end_date_str)