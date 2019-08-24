from unittest import TestCase

from config import logger_factory
from services.EquityToLegacyDataConversionService import EquityToLegacyDataConversionService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestEquityToLegacyDataConversionService(TestCase):

  def test_get_symbol(self):
    # Arrange
    num_records = 253 * 2
    latest_date = date_utils.parse_datestring('2019-05-23')

    # Act
    df = EquityToLegacyDataConversionService.get_symbol("GOOG", num_records=num_records, latest_date=latest_date)

    logger.info(f"Cols: {df.columns}")

    # Assert
    assert(df.shape[0] == num_records)

    date_str = df.iloc[-1]['Date']
    assert(date_str == '2019-05-23')

  def test_get_many_symbols(self):
    # Arrange
    num_records = 253 * 2
    latest_date = date_utils.parse_datestring('2019-05-23')

    symbols = ['GOOG',
     'FB',
     'LB',
     'MTDR',
     'CPRT',
     'FSV',
     'AMD',
     'TSLA',
     'SINA',
     'GWR']

    # Act
    for s in symbols:
      logger.info(f"Getting {s} ...")
      df = EquityToLegacyDataConversionService.get_symbol(s, num_records=num_records, latest_date=latest_date)

      assert(df.shape[0] == num_records)