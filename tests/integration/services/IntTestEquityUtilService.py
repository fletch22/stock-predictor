import os
from datetime import timedelta, datetime
from unittest import TestCase

import statistics
from config.logger_factory import logger_factory
from services import eod_data_service, split_eod_data_to_files_service, file_services
from services.EquityUtilService import EquityUtilService
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestEquityUtilService(TestCase):

  def test_find_missing_days(self):
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    end_date = date_utils.parse_datestring("2019-06-14")
    until_date = end_date + timedelta(days=3)
    missing = eod_data_service.find_and_download_missing_days(df, until_date)

    assert(len(missing) > 0)
    supp_filepath = missing[0]['supplemental_file']
    assert(os.path.exists(supp_filepath))

  def test_merge(self):
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.LARGE)

    df_merged = eod_data_service.merge_shar_equity_price_data(df)

    assert(df_merged.shape[0] > df.shape[0])

  def test_get_todays_merged_shar_data(self):
    # Arrange
    df_merged = eod_data_service.get_todays_merged_shar_data()
    df_merged.sort_values(by=['date'], inplace=True)

    oldest = df_merged.iloc[-1,:]['date']

    logger.info(f"Last date: {oldest}")

    # Assert
    assert(df_merged.shape[0] > 0)

  def test_spark_split_shar_equity_to_ticker_files(self):
    # Arrange
    # Act
    stock_infos = split_eod_data_to_files_service.split_shar_equity_to_ticker_files()

    # Assert
    assert(len(stock_infos) > 0)

  def test_get_tesla(self):
    # Arrange
    # Act
    df = EquityUtilService.get_df_from_ticker_path('tsla', False)

    # Assert
    assert(df.shape[0] > 1000)

  def test_select_single_day_equity_data(self):
    # Arrange
    yield_date = date_utils.parse_datestring('2019-08-16')
    trading_days_avail = 1000
    min_price = 5.0
    amount_to_spend = 25000

    # Act
    df = EquityUtilService.select_single_day_equity_data(yield_date, trading_days_avail=trading_days_avail, min_price=min_price, amount_to_spend=amount_to_spend)

    # Assert
    assert(df.shape[0] > 1000)

  def test_filter_symbol_high_variability(self):
    # Arrange
    expected_remained = 'c:/foo/1_ZYTO_2018-12-31.png'
    file_paths = [
      'c:/foo/0_ZUO_2018-12-31.png',
      expected_remained
    ]

    # Act
    result_list = EquityUtilService.filter_high_variability(file_paths, False)

    # Assert
    assert(len(result_list) == 1)
    assert(result_list[0] == expected_remained)

  def test_get_average_volatility(self):
    end_date = datetime.now()
    df = EquityUtilService.get_merged_mean_stdev(end_date, 253, price_min=50.00)

    logger.info(f"Mean stdev: {statistics.mean(df['price'].values.tolist())}")

  def test_add_realtime_prices(self):
    # Arrange
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)
    symbols = ['AAPL', 'A', 'IBM', 'MSFT']

    df = df[df['ticker'].isin(symbols)]

    ibm_info = RealtimeEquityPriceService.get_prices(symbols=['IBM'])[0]
    ibm_price_expected = float(ibm_info['price'])

    # Act
    df_added = EquityUtilService.add_realtime_price(df)

    # Assert
    assert(df_added.shape[0] > 0)

    today_str = date_utils.get_standard_ymd_format(datetime.now())
    num_today_rows = df_added[df_added['date'] == today_str].shape[0]
    assert(num_today_rows == len(symbols))

    ibm_row = df_added[(df_added['date'] == today_str) & (df_added['ticker'] == 'IBM')]
    ibm_close_actual = ibm_row.iloc[0]['close']

    assert(ibm_price_expected == ibm_close_actual)




