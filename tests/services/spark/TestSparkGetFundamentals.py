from unittest import TestCase

from config import logger_factory
from services.spark import spark_get_fundamentals
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestSparkGetFundamentals(TestCase):

  def test_spark_get_fundamentals(self):
    # Arrange
    symbol = "ibm"

    symbol_info = {}
    symbol_info["symbol"] = symbol
    symbol_info["offsets"] = [100, 200]
    symbol_info['trading_days_span'] = 1000
    symbol_info["start_date"] = date_utils.parse_datestring("2013-01-02")
    symbol_info["end_date"] = date_utils.parse_datestring("2019-01-02")
    symbol_info["desired_fundamentals"] = ['pe', 'ev', 'eps']

    # Act
    results = spark_get_fundamentals.do_spark([symbol_info], num_slices=1)

    # Assert
    assert(len(results) == 1)

    symb_fundie = results[0]

    assert(symbol == symb_fundie['symbol'])
    offset_info = symb_fundie['offset_info']

    first_offset = offset_info[100]
    first_fundy_pe = first_offset['pe']

    assert(first_fundy_pe == 14.15)

    second_offset = offset_info[200]
    second_fundy_pe = second_offset['pe']

    assert (second_fundy_pe == 11.957)

    logger.info(f"Results: {results}")
