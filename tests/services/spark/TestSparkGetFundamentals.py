import os
from unittest import TestCase

from config import logger_factory
from services.spark import spark_get_fundamentals
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestSparkGetFundamentals(TestCase):

  def test_spark_get_fundamentals(self):
    # Arrange
    # os.environ['SPARK_HOME'] = "\\Users\\Chris\\workspaces\\open_source\\spark-2.4.3-bin-hadoop2.7"

    symbol = "msft"
    second_offset = 1000

    symbol_info = {}
    symbol_info["symbol"] = symbol
    symbol_info["offsets"] = [100, second_offset]
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

    assert(first_fundy_pe == 20.958)

    second_offset = offset_info[second_offset]
    second_fundy_pe = second_offset['pe']

    assert (second_fundy_pe == 23.245)

    logger.info(f"Results: {results}")

  def test_spark_process_sample_info(self):
    # Arrange
    symbol = "msft"
    second_offset = 300

    symbol_info = {}
    symbol_info["symbol"] = symbol
    symbol_info["offsets"] = [100, second_offset]
    symbol_info['trading_days_span'] = 1000
    symbol_info["start_date"] = date_utils.parse_datestring("2013-01-02")
    symbol_info["end_date"] = date_utils.parse_datestring("2019-01-02")
    symbol_info["desired_fundamentals"] = ['pe', 'ev', 'eps']

    results = spark_get_fundamentals.spark_process_sample_info(symbol_info)

    logger.info(f"s: {results}")

    assert(results['offset_info'][100]['pe'] == 20.958)
