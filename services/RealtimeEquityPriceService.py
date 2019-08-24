from datetime import datetime

import config
from config import logger_factory
from utils import list_utils, date_utils
import requests

logger = logger_factory.create_logger(__name__)

class RealtimeEquityPriceService():

  @classmethod
  def get_prices(cls, symbols: list):
    chunk_size = 20 # Maximum number of requests
    symbols_chunks = []
    while len(symbols) > 0:
      symbols_chunks.append(symbols[:chunk_size])
      del symbols[:chunk_size]

    price_results = list()
    for chunk in symbols_chunks:
      price_results.extend(cls.get_prices_for_chunks(chunk))

    return price_results

  @classmethod
  def get_prices_for_chunks(cls, symbols: list):

    concat_symbols = ",".join(symbols)

    uri = f"https://api.worldtradingdata.com/api/v1/stock?symbol={concat_symbols}&api_token={config.constants.WTD_KEY}"
    response = requests.get(uri)

    logger.info(response.status_code)

    prices = response.json()

    return prices['data']

  @classmethod
  def get_historical_price(cls, symbol: str, start_date: datetime, end_date: datetime):

    start_date_str = date_utils.get_standard_ymd_format(start_date)
    end_date_str = date_utils.get_standard_ymd_format(end_date)

    uri = f"https://api.worldtradingdata.com/api/v1/history?symbol={symbol}&date_from={start_date_str}&date_to={end_date_str}&api_token={config.constants.WTD_KEY}"
    response = requests.get(uri)

    prices = response.json()

    return prices