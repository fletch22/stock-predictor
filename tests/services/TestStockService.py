import os
from datetime import datetime
from unittest import TestCase

from services.Eod import Eod

import config
from config import logger_factory
from services import chart_service, file_services
from services.BasicSymbolPackage import BasicSymbolPackage
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService
from utils import random_utils, date_utils
from utils.date_utils import STANDARD_DAY_FORMAT
import shutil

logger = logger_factory.create_logger(__name__)

class TestStockService(TestCase):

  def test_cache_filename_composition(self):
    # Arrange
    stock_service = StockService()
    symbol = "goog"
    start_date = datetime.strptime("2222-04-01", STANDARD_DAY_FORMAT)
    num_days = 100

    # Act
    filename = stock_service.get_cache_filename(symbol, start_date, num_days)

    # Assert
    print(filename)
    assert filename is not None
    assert filename == 'goog_2222-04-01_100.csv'

  def test_load_random_stocks(self):
    # Arrange
    df = EquityUtilService.get_shar_equity_data()
    expected_num_to_load = 7

    unique_symbols = df["ticker"].unique().tolist()
    sel_symbols = random_utils.select_random_from_list(unique_symbols, expected_num_to_load)
    basic_symbol_package = BasicSymbolPackage()

    # Act
    for symbol in sel_symbols:
      basic_symbol_package.add_symbol(symbol, "2011-01-01", 30)

    # Assert
    assert(len(basic_symbol_package.data) == expected_num_to_load)

  def test_get_plot_and_save_many(self):
    # Arrange
    prediction_dir = os.path.join(config.constants.CACHE_DIR, "prediction_test")
    if os.path.exists(prediction_dir):
      shutil.rmtree(prediction_dir)

    df_good, df_bad = StockService.get_sample_data(prediction_dir, 120000, sample_file_size=False, persist_data=True)

    logger.info(f"Columns: {df_good.columns}")

    graph_dir = os.path.join(prediction_dir, "graphed")
    os.makedirs(graph_dir, exist_ok=True)

    # file_path = os.path.join(prediction_dir, "up_1.0_pct.csv")
    # chart_service.plot_and_save_from_single_file(graph_dir, file_path)

    chart_service.plot_and_save_for_learning(df_good, graph_dir, category="1")
    chart_service.plot_and_save_for_learning(df_bad, graph_dir, category="0")

    assert (True)

  def test_rename(self):
    par_dir = os.path.join(config.constants.CACHE_DIR, "prediction_test", "graphed - Copy")
    prediction_dir = os.path.join(par_dir, "0")

    filepaths = file_services.walk(prediction_dir)

    for idx, f in enumerate(filepaths):
      new_path = os.path.join(prediction_dir, f"{idx}.png")
      os.rename(f, new_path)
      # if idx > 10:
      #   break

  def test_get_result_from_date(self):
    # Arrange
    df = EquityUtilService.get_shar_equity_data(sample_file_size=True)
    symbol = "AAPL"
    date = date_utils.parse_datestring("2019-06-14")

    # Act
    eod_info = StockService.get_eod_of_date(symbol, date, df=df)

    # Assert
    assert(eod_info["symbol"] == symbol)
    assert(eod_info["date"] == date)
    assert(194.15 == eod_info["bet_price"])

  def test_get_sample_infos(self):
    # Arrange
    amount_to_spend = 250
    num_days_avail = 2
    min_price = 5.0
    df_g_filtered = StockService._get_and_prep_equity_data(amount_to_spend, num_days_avail, min_price, SampleFileTypeSize.SMALL)

    min_samples = 33

    # Act
    sample_info = StockService.get_sample_infos(df_g_filtered, num_days_avail, min_samples)

    # Assert
    total_offsets = 0
    for key in sample_info.keys():
      num_offsets = len(sample_info[key]["offsets"])
      total_offsets += num_offsets

    assert(total_offsets == min_samples)

  def test_yield_function(self):
    # Arrange
    symbol = "IBM"
    df = EquityUtilService.get_shar_equity_data()

    df_day_before = df[(df["ticker"] == symbol) & (df["date"] >= "2013-01-01") & (df["date"] <= "2013-01-31")]
    date_before = date_utils.parse_datestring("2013-02-01")
    eod_data_before = StockService.get_eod_of_date(symbol, date_before, df_day_before)

    logger.info(f"Close: {eod_data_before[Eod.CLOSE]}")

    df_yield_day = df[(df["ticker"] == symbol) & (df["date"] >= "2013-01-15") & (df["date"] <= "2013-02-01")]
    date_yield_day = date_utils.parse_datestring("2013-02-01")
    eod_data = StockService.get_eod_of_date(symbol, date_yield_day, df_yield_day)

    logger.info(f"Bet_price: {eod_data['bet_price']}")

    assert(eod_data_before[Eod.CLOSE] == eod_data["bet_price"])

  def test_get_stock_history_for_day(self):
    # Arrange
    amount_to_spend = 25000
    num_days_avail = 1000
    min_price = 5.0
    end_date = date_utils.parse_datestring("2019-07-17")

    df = EquityUtilService.get_shar_equity_data(SampleFileTypeSize.LARGE)
    # df = EquityUtilService.get_shar_equity_data(SampleFileTypeSize.SMALL)

    logger.info(f"Found {len(df['ticker'].unique().tolist())} tickers.")

    df_grouped = df.groupby('ticker')

    df_g_filtered = df_grouped.filter(lambda x: StockService.filter_equity_basic_criterium(amount_to_spend, num_days_avail, min_price, x))

    logger.info(f"df_g_filtered {len(df_g_filtered['ticker'].unique().tolist())} tickers.")

    parent_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "daily")
    os.makedirs(parent_dir, exist_ok=True)
    save_dir = file_services.create_unique_folder(parent_dir, "bet")

    # Act
    StockService.get_stock_history_for_day(df_g_filtered, 1000, save_dir, False, end_date)