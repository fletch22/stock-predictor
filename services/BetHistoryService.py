import os
from datetime import datetime

import config

import pandas as pd

from config.logger_factory import logger_factory
from utils import date_utils

logger = logger_factory.create_logger(__name__)

NUM_SHARES_COL = 'num_shares'
TICKER_COL = 'ticker'
PURCHASE_PRICE_COL = 'purchase_price'
DATE_COL = 'date'

dataframe_columns = [NUM_SHARES_COL, TICKER_COL, PURCHASE_PRICE_COL, DATE_COL]

class BetHistoryService:
  def __init__(self):
    self.csv_path = config.constants.BET_HIST_CSV_PATH

    self.df = None
    if os.path.exists(self.csv_path):
      self.df = pd.read_csv(self.csv_path)


  def add_bet(self, num_shares: float, symbol: str, purchase_price: float, date: datetime):
    date_str = date_utils.get_standard_ymd_format(date)

    row = {NUM_SHARES_COL: num_shares, TICKER_COL: symbol, PURCHASE_PRICE_COL: purchase_price, DATE_COL: date_str}

    df_new = pd.DataFrame([row])
    if self.df is None:
      self.df = df_new
    else:
      self.df = pd.concat([self.df, df_new], ignore_index=True, sort=False)

    if symbol != 'fake':
      self.df.to_csv(self.csv_path, index=False)

