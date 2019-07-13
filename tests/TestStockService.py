from unittest import TestCase

from services.StockService import StockService
from datetime import datetime, timedelta


class TestStockService(TestCase):

  def test_cache_filename_compositiion(self):
    # Arrange
    stock_service = StockService()
    symbol = "goog"
    start_date = datetime.now()
    end_date = start_date + timedelta(days = 1)

    # Act
    filename = stock_service.get_cache_filename(symbol, start_date, end_date)

    # Assert
    print(filename)
    assert filename is not None
    assert filename == 'goog-2019-13-2019-13.csv'