from unittest import TestCase

from config.logger_factory import logger_factory
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize

logger = logger_factory.create_logger(__name__)

class TestEquityUtilService(TestCase):

  def test_split(self):
    # Arrange
    # Act
    symbols_extract = EquityUtilService.split_shar_equity_to_ticker_files(SampleFileTypeSize.SMALL)

    # Assert
    logger.info(f"Num extracted: {len(symbols_extract)}")
    assert(len(symbols_extract) == 6469)

  def test_foo(self):
    # print("f")
    symbols_extract = EquityUtilService.split_shar_equity_to_ticker_files(SampleFileTypeSize.LARGE)

