from datetime import datetime
from typing import List

import pandas as pd
import requests

import config
from config import logger_factory
from services.RedisService import RedisService
from services.realtime_prices.RealtimeEquityInfo import RealtimeEquityInfo
from utils import date_utils
import numpy as np

logger = logger_factory.create_logger(__name__)
API_BASE_URL = "https://api.worldtradingdata.com/api/v1"


class RealtimeEquityPriceService():

  # Example returns [{'symbol': 'A', 'name': 'Agilent Technologies, Inc.', 'currency': 'USD', 'price': '77.97', 'price_open': '78.22', 'day_high': '78.74', 'day_low': '77.91', '52_week_high': '82.27', '52_week_low': '61.01', 'day_change': '-0.10', 'change_pct': '-0.13', 'close_yesterday': '78.07', 'market_cap': '24284694528', 'volume': '4603693', 'volume_avg': '2705537', 'shares': '309468000', 'stock_exchange_long': 'New York Stock Exchange', 'stock_exchange_short': 'NYSE', 'timezone': 'EDT', 'timezone_name': 'America/New_York', 'gmt_offset': '-14400', 'last_trade_time': '2019-09-20 16:07:28'}, {'symbol': 'AAPL', 'name': 'Apple Inc.', 'currency': 'USD', 'price': '217.73', 'price_open': '221.38', 'day_high': '222.56', 'day_low': '217.48', '52_week_high': '233.47', '52_week_low': '142.00', 'day_change': '-3.23', 'change_pct': '-1.46', 'close_yesterday': '220.96', 'market_cap': '975966044160', 'volume': '32221071', 'volume_avg': '29366862', 'shares': '4519179776', 'stock_exchange_long': 'NASDAQ Stock Exchange', 'stock_exchange_short': 'NASDAQ', 'timezone': 'EDT', 'timezone_name': 'America/New_York', 'gmt_offset': '-14400', 'last_trade_time': '2019-09-20 16:00:01'}, {'symbol': 'IBM', 'name': 'International Business Machines Corporation', 'currency': 'USD', 'price': '141.88', 'price_open': '143.25', 'day_high': '143.83', 'day_low': '141.82', '52_week_high': '154.36', '52_week_low': '105.94', 'day_change': '-1.09', 'change_pct': '-0.76', 'close_yesterday': '142.97', 'market_cap': '125687947264', 'volume': '5198612', 'volume_avg': '2937075', 'shares': '885875008', 'stock_exchange_long': 'New York Stock Exchange', 'stock_exchange_short': 'NYSE', 'timezone': 'EDT', 'timezone_name': 'America/New_York', 'gmt_offset': '-14400', 'last_trade_time': '2019-09-20 16:02:27'}]
  @classmethod
  def get_equities(cls, symbols: list) -> List[dict]:
    chunk_size = 20  # Maximum number of requests
    symb_orig = symbols.copy()
    symbols_chunks = []
    while len(symb_orig) > 0:
      symbols_chunks.append(symb_orig[:chunk_size])
      del symb_orig[:chunk_size]

    price_results = list()
    for chunk in symbols_chunks:
      logger.info(f"Getting: {chunk}")
      price_results.extend(cls.get_prices_for_chunks(chunk))

    return price_results

  @classmethod
  def get_prices_for_chunks(cls, symbols: list):

    concat_symbols = ",".join(symbols)

    uri = f"{API_BASE_URL}/stock?symbol={concat_symbols}&api_token={config.constants.WTD_KEY}"
    response = requests.get(uri)

    logger.info(response.status_code)

    prices = response.json()

    if 'data' not in prices.keys():
      logger.info(f"Error: JSON: {prices}")
      raise Exception("prices key not found.")

    return prices['data']

  @classmethod
  def get_historical_price(cls, symbol: str, start_date: datetime, end_date: datetime):

    start_date_str = date_utils.get_standard_ymd_format(start_date)
    end_date_str = date_utils.get_standard_ymd_format(end_date)

    uri = f"{API_BASE_URL}/history?symbol={symbol}&date_from={start_date_str}&date_to={end_date_str}&api_token={config.constants.WTD_KEY}"
    response = requests.get(uri)

    prices = response.json()

    return prices

  @classmethod
  def get_recent_ticker_list(cls):
    today_str = date_utils.get_standard_ymd_format(datetime.now())
    redis_key = f"{cls.__name__}-recent_tickers"

    redis_service = RedisService()

    needs_update = False
    recent_tickers = redis_service.read_as_json(redis_key)
    if recent_tickers is None:
      needs_update = True
    else:
      date_str = recent_tickers['date']
      if date_str != today_str:
        needs_update = True

    if needs_update:
      recent_tickers = {
        'date': today_str,
        'tickers': cls._get_online_ticker_list()
      }
      redis_service.write_as_json(redis_key, recent_tickers)

    return recent_tickers['tickers']

  @classmethod
  def get_ticker_list_from_file(cls):
    file_path = config.constants.WTD_STOCK_LIST_PATH
    df = pd.read_csv(file_path)

    return df['Symbol'].unique().tolist()

  @classmethod
  def _get_online_ticker_list(cls):
    uri = f"{API_BASE_URL}/ticker_list?api_token={config.constants.WTD_KEY}"
    response = requests.get(uri)

    ticker_list = response.json()

    return ticker_list

  @classmethod
  def get_and_clean_realtime_equities(cls, symbols: List[str]) -> List[RealtimeEquityInfo]:
    equity_info = RealtimeEquityPriceService.get_equities(symbols=symbols)

    rt_equity_info_list = []
    for ei in equity_info:
      logger.info(f"ei: {ei}")
      rt_equity_info = RealtimeEquityInfo()
      rt_equity_info.symbol = ei['symbol']
      rt_equity_info.open = cls.convert_not_avail_to_nan(ei, 'price_open')
      rt_equity_info.high_to_date = cls.convert_not_avail_to_nan(ei, 'day_high')
      rt_equity_info.low_to_date = cls.convert_not_avail_to_nan(ei, 'day_low')
      rt_equity_info.current_price = float(ei['price'])
      rt_equity_info.last_trade_time = date_utils.convert_wtd_nyc_date_to_std(ei['last_trade_time'])

      volume_str = ei['volume']
      rt_equity_info.volume_to_date = 0 if volume_str == 'N/A' else float(volume_str)
      rt_equity_info_list.append(rt_equity_info)

    return rt_equity_info_list

  @classmethod
  def convert_not_avail_to_nan(cls, dictionary, dictionary_key):
    value_str = dictionary[dictionary_key]
    return np.NaN if value_str == 'N/A' else float(value_str)
