from datetime import datetime
from datetime import datetime
from unittest import TestCase

from config import logger_factory
from learning_set.LearningSetMetaData import LearningSetMetaData
from prediction.PredictionRosebud import PredictionRosebud
from services import eod_data_service
from services.BasicSymbolPackage import BasicSymbolPackage
from services.Eod import Eod
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService
from utils import random_utils, date_utils
from utils.date_utils import STANDARD_DAY_FORMAT

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
        df = eod_data_service.get_shar_equity_data()
        expected_num_to_load = 7

        unique_symbols = df["ticker"].unique().tolist()
        sel_symbols = random_utils.select_random_from_list(unique_symbols, expected_num_to_load)
        basic_symbol_package = BasicSymbolPackage()

        # Act
        for symbol in sel_symbols:
            basic_symbol_package.add_symbol(symbol, "2011-01-01", 30)

        # Assert
        assert (len(basic_symbol_package.data) == expected_num_to_load)

    def test_get_result_from_date(self):
        # Arrange
        df = eod_data_service.get_shar_equity_data(sample_file_size=SampleFileTypeSize.SMALL)
        symbol = "AAPL"
        yield_date = date_utils.parse_std_datestring("2019-06-14")

        # Act
        eod_info = StockService.get_eod_of_date(symbol, yield_date)

        # Assert
        assert (eod_info["symbol"] == symbol)
        assert (eod_info["date"] == yield_date)
        assert (194.15 == eod_info["bet_price"])

    def test_get_sample_infos(self):
        # Arrange
        lsm = LearningSetMetaData()
        lsm.trading_days_span = 2
        lsm.min_price = 5.0
        lsm.volatility_min = 10
        lsm.min_volume = 100000
        lsm.start_date = None
        lsm.end_date = None

        df_g_filtered = StockService.get_and_prep_equity_data(lsm=lsm)

        min_samples = 33

        # Act
        sample_info = StockService.get_sample_infos(df_g_filtered, lsm.trading_days_span, min_samples)

        # Assert
        total_offsets = 0
        for key in sample_info.keys():
            num_offsets = len(sample_info[key]["offsets"])
            total_offsets += num_offsets

        assert (total_offsets == min_samples)

    def test_yield_function(self):
        # Arrange
        symbol = "IBM"
        df = eod_data_service.get_shar_equity_data()

        df_day_before = df[(df["ticker"] == symbol) & (df["date"] >= "2013-01-01") & (df["date"] <= "2013-01-31")]
        date_before = date_utils.parse_std_datestring("2013-02-01")
        eod_data_before = StockService.get_eod_of_date(symbol, date_before)

        logger.info(f"Close: {eod_data_before[Eod.CLOSE]}")

        df_yield_day = df[(df["ticker"] == symbol) & (df["date"] >= "2013-01-15") & (df["date"] <= "2013-02-01")]
        date_yield_day = date_utils.parse_std_datestring("2013-02-01")
        eod_data = StockService.get_eod_of_date(symbol, date_yield_day)

        logger.info(f"Bet_price: {eod_data['bet_price']}")

        assert (eod_data_before[Eod.CLOSE] == eod_data["bet_price"])

    def test_get_and_prep_equity_data_one_day(self):
        # Arrange
        prediction_rosebud = PredictionRosebud()
        prediction_rosebud.min_price = 5.0
        prediction_rosebud.num_days_avail = 1000
        yield_date_str = "2019-08-15"
        prediction_rosebud.yield_date = date_utils.parse_std_datestring(yield_date_str)
        prediction_rosebud.volatility_min = 2.79

        # Act
        df = StockService.get_and_prep_equity_data_one_day(prediction_rosebud=prediction_rosebud)

        # Assert
        df_temp_count = df[df['date'] <= yield_date_str]
        logger.info(f"df_temp {df_temp_count.shape[0]}")
        assert (df_temp_count.shape[0] == df.shape[0])

        symbols_with_date = df_temp_count['ticker'].unique().tolist()
        symbols_with_date.sort()

        logger.info(f"Num symbols with date: {len(symbols_with_date)}")
        symbols_total = df['ticker'].unique().tolist()
        symbols_total.sort()

        logger.info(f"Num symbols total: {len(symbols_total)}")
        assert (len(symbols_total) > 0)
        assert (symbols_total == symbols_with_date)

    def test_count_equity(self):
        df = eod_data_service.get_shar_equity_data(sample_file_size=SampleFileTypeSize.LARGE)
        # df = eod_data_service.get_shar_equity_data(sample_file_size=SampleFileTypeSize.SMALL)

        logger.info(f'Columns: {df.columns}')

        df = df[df['ticker'] == 'VOD']

        logger.info(f'{df["date"].min()} thru {df["date"].max()}')

        logger.info(f'Num rows: {df.shape[0]}')
