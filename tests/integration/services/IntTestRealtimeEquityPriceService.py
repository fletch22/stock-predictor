from unittest import TestCase

from config import logger_factory
from services.RealtimeEquityPriceService import RealtimeEquityPriceService

logger = logger_factory.create_logger(__name__)

class TestRealtimeEquityPriceService(TestCase):

  def test_get_realtime_prices(self):
    # Arrange
    symbols = ['AAPL', 'A', 'IBM']

    # Act
    prices = RealtimeEquityPriceService.get_prices(symbols)

    logger.info(f"Prices: {prices}")

    # Assert
    assert(prices is not None)

