from typing import Dict, List
from unittest import TestCase

from config import logger_factory
from services.RealtimeEquityPriceService import RealtimeEquityPriceService

logger = logger_factory.create_logger(__name__)

class TestRealtimeEquityPriceService(TestCase):

  def test_get_realtime_prices(self):
    # Arrange
    symbols = ['AAPL', 'A', 'IBM']

    # Act
    prices = RealtimeEquityPriceService.get_equities(symbols)

    logger.info(f"Prices: {prices}")

    # Assert
    assert(prices is not None)

  def test_get_and_clean_realtime_equities(self):
    # Arrange
    symbols = ['AAPL', 'A', 'IBM']

    # Act
    equity_info = RealtimeEquityPriceService.get_and_clean_realtime_equities(symbols)

    logger.info(f"Prices: {equity_info}")

    # Assert
    assert(equity_info is not None)
    assert(len(equity_info) > 0)
    rt_equity_info = [e for e in equity_info if e.symbol == 'AAPL'][0]
    assert(rt_equity_info.symbol == 'AAPL')

  def test_foo(self):
    # symbols = ['IBM', 'PNC-PP', 'TCO-PK', 'RNR-PE', 'TCO-PJ', 'CMO-PE', 'ANH-PB', 'ZB-PA', 'BBT-PF', 'DLR-PG', 'AGM-PA', 'GS-PA', 'HLM-P', 'RNR-PC', 'SLG-PI', 'BFS-PC', 'PEB-PC']
    symbols = ['TCO.PJ']
    prices = RealtimeEquityPriceService.get_equities(symbols)

    assert(len(symbols) == len(prices))

  def test_ticker_list_online(self):
    # Arrange
    # Act
    ticker_list = RealtimeEquityPriceService._get_online_ticker_list()

    # Assert
    assert(len(ticker_list) > 1000)

  def test_ticker_list(self):
    # Arrange
    # Act
    ticker_list = RealtimeEquityPriceService.get_recent_ticker_list()

    logger.info(f"Ticker list: {ticker_list}")

    # Assert
    assert(len(ticker_list) > 1000)

  def test_ticker_list_from_file(self):
    # Arrange
    # Act
    ticker_list = RealtimeEquityPriceService.get_ticker_list_from_file()

    logger.info(f"Ticker list: {ticker_list}")

    # Assert
    assert(len(ticker_list) > 1000)

