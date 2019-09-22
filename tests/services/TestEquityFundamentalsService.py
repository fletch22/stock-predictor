import os
from datetime import datetime
from unittest import TestCase

import config
from config import logger_factory
from services.equities import equity_fundamentals_service

logger = logger_factory.create_logger(__name__)


class TestEquityFundamentalsService(TestCase):

  def test_get_pe(self):
    # Arrange
    # Act
    fundies = equity_fundamentals_service.get_multiple_values("goog", desired_values=["pe"], bet_date=datetime.now(), df=None, translate_to_hdfs=False)

    pe = fundies['pe']

    logger.info(f"P/E: {pe}")

    # Assert
    assert (pe > 20)

  def test_get_scaled_fundamentals(self):
    # Arrange
    upper_bound = 100
    lower_bound = 0
    fundy_infos = [
      {'symbol': 'ibm', 'offset_info': {100: {'pe': 0, 'ev': 3}, 200: {'pe': 5, 'ev': 7}}},
      {'symbol': 'msft', 'offset_info': {101: {'pe': 2, 'ev': 4}, 2222: {'pe': 10, 'ev': 10}}}
    ]

    package_path = os.path.join(config.constants.CACHE_DIR)

    # Act
    result = equity_fundamentals_service.get_scaled_fundamentals(fundy_infos, package_path)

    # Assert
    logger.info(f"Res: {result}")

    ibm = result[0]
    msft = result[1]

    assert(ibm['symbol'] == 'ibm')

    ibm_fun_100 = ibm['offset_info'][100]
    assert(ibm_fun_100['pe'] == 0.0)
    assert (ibm_fun_100['ev'] == 0.0)

    assert (msft['symbol'] == 'msft')

    msft_fun_101 = msft['offset_info'][101]
    assert (msft_fun_101['pe'] == 20.0)
    assert (msft_fun_101['ev'] == 14.285714285714285)

  def test_ensure_haircut(self):
    # Arrange
    # Act
    result_1 = equity_fundamentals_service._adjust_special_fundamentals('pe', -1000)
    result_2 = equity_fundamentals_service._adjust_special_fundamentals('pe', 1000)
    result_3 = equity_fundamentals_service._adjust_special_fundamentals('pe', 50)

    # Assert
    assert(result_1 == -100)
    assert (result_2 == 200)
    assert (result_3 == 50)

  def test_create_min_max_scalers(self):
    # Arrange
    fundy_infos = [{'symbol': 'ibm', 'offset_info': {100: {'pe': 14.15, 'ev': 198593033631.0}, 200: {'pe': 11.957, 'ev': 169182898289.0}}}]
    package_path = os.path.join(config.constants.TEST_PACKAGE_FOLDER)

    # Act
    list_min_maxer = equity_fundamentals_service.create_min_maxer(fundy_infos=fundy_infos, package_path=package_path)

    scaled_value = list_min_maxer.scale(14.15, 'pe')

    # Assert
    assert(scaled_value == 1.0)



