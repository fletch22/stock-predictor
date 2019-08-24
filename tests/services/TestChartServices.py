import os
from datetime import datetime
from unittest import TestCase

from sklearn.preprocessing import MinMaxScaler

import config
from config import logger_factory
from services import chart_service
import pandas as pd

from services.StockService import StockService
from services.equities import equity_fundamentals_service

logger = logger_factory.create_logger(__name__)

class TestChartServices(TestCase):

  def test_chart_drawing(self):
    # Arrange
    expected_output_path = os.path.join(config.constants.CACHE_DIR, "1_IBM_2019-06-14.png")
    os.remove(expected_output_path)
    file_path = os.path.join(config.constants.QUANDL_DIR, "ibm_sample_1000.csv")

    df = pd.read_csv(file_path)

    save_dir = os.path.join(config.constants.CACHE_DIR)

    # Act
    chart_service.plot_and_save_for_learning(df, save_dir, "1")

    # Assert
    assert(os.path.exists(expected_output_path))

  def test_decorated_chart(self):
    # Arrange
    save_dir = os.path.join(config.constants.CACHE_DIR)

    symbol = "FLXS"
    df = StockService.get_symbol_df(symbol, translate_file_path_to_hdfs=False)
    df.sort_values(by=['date'], inplace=True)

    fundies = equity_fundamentals_service.get_multiple_values(symbol, desired_values=['pe', 'ev'], bet_date=datetime.now(), df=None, translate_to_hdfs=False)

    pe = fundies['pe']
    ev = fundies['ev']

    # full_data_service = FullDataEquityFundamentalsService()
    # full_data_service.filter(['IBM', symbol, 'AAPL', 'MSFT'])
    # pe_min, pe_max = full_data_service.get_min_max_pe()

    pe_min = -100
    pe_max = 200
    pe = pe_max if pe > pe_max else pe
    pe = pe_min if pe < pe_min else pe

    logger.info(f"Original pe: {pe}")

    pe_scaled = ((pe + 100) / (pe_max + 100)) * 100

    logger.info(f"pe_scaled: {pe_scaled}")

    fundy = {
      "pe": pe_scaled,
      "eps": 50
    }

    category = "1"
    symbol = "ibm"
    yield_date_str = "2019-07-16"

    plt = chart_service.render_decorated_chart(df, fundy=fundy, category=category, symbol=symbol, yield_date_str=yield_date_str,
                                                   save_dir=save_dir, translate_save_path_hdfs=False)

    plt.show()