import alpaca_trade_api as tradeapi

import config


class AlpacaService():

  def __init__(self):
    credentials = config.constants.ALPACA_CREDENTIALS
    api_cred = credentials['PAPER-API']
    API_KEY = api_cred['API-KEY-ID']
    API_SECRET = api_cred['API-SECRET-KEY']
    API_BASE_URL = api_cred['API_BASE_URL']

    self.api = tradeapi.REST(API_KEY, API_SECRET, API_BASE_URL, 'v2')

  def list_positions(self):
    return self.api.list_positions()

  def get_account(self):
    return self.api.get_account()