import os
from datetime import timedelta, datetime
from unittest import TestCase

import statistics

import config
from categorical.BinaryCategoryType import BinaryCategoryType
from config.logger_factory import logger_factory
from services import eod_data_service, split_eod_data_to_files_service, file_services, project_upload_service
from services.EquityUtilService import EquityUtilService
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils
import pandas as pd

logger = logger_factory.create_logger(__name__)

class TestEquityUtilService(TestCase):

  def test_find_missing_days(self):
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    end_date = date_utils.parse_std_datestring("2019-06-14")
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
    yield_date = date_utils.parse_std_datestring('2019-08-16')
    trading_days_avail = 1000
    min_price = 5.0
    min_volume = 100000
    volatility_min = 2.8

    # Act
    df = EquityUtilService.select_single_day_equity_data(yield_date=yield_date,
                                                         trading_days_avail=trading_days_avail,
                                                         min_price=min_price, min_volume=min_volume, volatility_min=volatility_min)

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

    ibm_info = RealtimeEquityPriceService.get_equities(symbols=['IBM'])[0]
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

  def test_get_highest_volumes(self):
    # Arrange
    df_merged = eod_data_service.get_todays_merged_shar_data()
    df_date = df_merged[df_merged['date'] == '2018-11-15']

    number_symbols = 100
    df_largest = df_date.nlargest(number_symbols, 'volume')
    top_volume = df_largest.iloc[0]['volume']
    bott_volume = df_largest.iloc[-1]['volume']

    logger.info(f"Vol range: {top_volume:,} - {bott_volume:,}")

    symbols = df_largest['ticker'].unique().tolist()

    symbols = sorted(symbols)
    logger.info(f"{symbols}")

    # Assert
    assert(df_largest.shape[0] > 0)

  def test_get_gains(self):
    # Arrange
    df = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)

    open_closes = []
    def trim_to_window(df_ticker):
      df_ticker.sort_values(by=['ticker'], inplace=True)
      df_ticker = df_ticker.iloc[-1000:,:]
      first = df_ticker.iloc[0]['open']

      sample = df_ticker.iloc[-1]
      symbol = sample['ticker']
      last = sample['close']

      open_closes.append({'symbol': symbol, 'diff': last-first})
      return df_ticker

    df = df.groupby("ticker").apply(lambda x: trim_to_window(x))

    results = [oc for oc in open_closes if oc['diff'] > 0]

    # logger.info(f"Foo: {df['foo'].unique().tolist()[0]}")

    # Assert
    # assert(df_merged.shape[0] > 0)

    # ['AAPL', 'ABEV', 'ACB', 'AKS', 'AMD', 'AR', 'AUY', 'BABA', 'BAC', 'BBBY', 'BBD', 'BCS', 'BMY', 'BSX', 'C', 'CGC', 'CGIX', 'CHK', 'CLF', 'CMCSA', 'CSCO', 'CX', 'CZR', 'DAL', 'DNR', 'ECA', 'ET', 'F', 'FAST', 'FB', 'FCX', 'GE', 'GGB', 'GM', 'GOLD', 'GOVX', 'GPRO', 'GSV', 'HAL', 'HMNY', 'HPQ', 'INFY', 'ING', 'INTC', 'ITUB', 'JD', 'JPM', 'KEY', 'KGC', 'KHC', 'KMI', 'LYG', 'M', 'MDR', 'MRO', 'MS', 'MSFT', 'MU', 'NAKD', 'NAT', 'NBR', 'NEM', 'NIO', 'NOK', 'NVDA', 'OAS', 'ON', 'ORCL', 'PBR', 'PCG', 'PFE', 'QEP', 'RF', 'RIG', 'ROKU', 'S', 'SAN', 'SCHW', 'SCON', 'SES', 'SIRI', 'SLB', 'SLS', 'SNAP', 'SWN', 'T', 'TEVA', 'TNK', 'UBER', 'VALE', 'VER', 'VZ', 'WEN', 'WFC', 'WLL', 'WORK', 'X', 'XOM', 'YNDX', 'ZNGA']
    # ['AAPL', 'ABBV', 'ABEV', 'ABT', 'ACB', 'AES', 'AG', 'AKS', 'AMAT', 'AMD', 'AUY', 'BABA', 'BAC', 'BBBY', 'BHGE', 'BMY', 'BP', 'C', 'CAG', 'CHK', 'CLDR', 'CMCSA', 'CPE', 'CRZO', 'CSCO', 'CSX', 'CTL', 'CZR', 'DNR', 'DOYU', 'EBAY', 'ECA', 'ERIC', 'ET', 'F', 'FB', 'FCEL', 'FCX', 'FRAN', 'FSM', 'GE', 'GHSI', 'GOLD', 'GPOR', 'HAL', 'HBAN', 'HL', 'INFY', 'INTC', 'ITUB', 'JNJ', 'JPM', 'KEY', 'KGC', 'KMI', 'MPW', 'MRK', 'MRO', 'MRVL', 'MS', 'MSFT', 'MU', 'MYDX', 'NBR', 'NFLX', 'NGD', 'NIO', 'NLY', 'NOK', 'NVDA', 'OAS', 'ORCL', 'PBR', 'PFE', 'QCOM', 'RF', 'RIG', 'ROKU', 'RRC', 'S', 'SAN', 'SENS', 'SIRI', 'SLB', 'SLS', 'SNAP', 'SWN', 'SYMC', 'T', 'TEVA', 'TRQ', 'TSLA', 'UNP', 'USB', 'VALE', 'VER', 'VISL', 'WFC', 'X', 'ZNGA']

  def test_balance_by_ticker_and_cat(self):
    # Arrange
    project_path = os.path.join(config.constants.CACHE_DIR, "test_balance")
    files = file_services.walk(project_path)

    symb_dict = {}
    for f in files:
      info = file_services.get_filename_info(f)
      symbol = info['symbol']

      symb_files = []
      if symbol in symb_dict.keys():
        symb_files = symb_dict[symbol]
      else:
        symb_dict[symbol] = symb_files

      symb_files.append(f)

    results = []
    for s in symb_dict.keys():
      symb_files = symb_dict[s]
      results.extend(project_upload_service.balance_files_by_category(symb_files))

    # Act
    # Arrange
    assert(len(results) == 2)
    ibm_1 = [f for f in results if os.path.basename(f).startswith(BinaryCategoryType.ONE)][0]
    info = file_services.get_filename_info(ibm_1)
    assert(info['symbol'] == 'IBM')

    ibm_0 = [f for f in results if os.path.basename(f).startswith(BinaryCategoryType.ZERO)][0]
    info = file_services.get_filename_info(ibm_0)
    assert (info['symbol'] == 'IBM')

    # ibm_0 = [f for f in result if f.startswith(BinaryCategoryType.ONE)]

