
import pandas as pd

import config
from services.ExchangeType import ExchangeType

GOOD_EXCHANGES = [ExchangeType.NYSE, ExchangeType.NASDAQ, ExchangeType.BATS, ExchangeType.IEX, ExchangeType.NYSEARCA, ExchangeType.NYSEMKT]

class TickerService():

  @classmethod
  def get_tickers_as_df(self):
    return pd.read_csv(config.constants.SHAR_TICKERS)

  @classmethod
  def get_tickers_list(cls):
    df = TickerService.get_tickers_as_df()
    return df[df['exchange'].isin(GOOD_EXCHANGES)]['ticker'].unique().tolist()

  @classmethod
  def merge_equities_with_eq_meta(cls, df_equities: pd.DataFrame, df_tickers: pd.DataFrame):
    return pd.merge(df_equities, df_tickers, on='ticker')

  @classmethod
  def filter_by_valid_stock_exchange(cls, df_equities: pd.DataFrame):
    symbols = cls.get_tickers_list()

    return df_equities[df_equities['ticker'].isin(symbols)]




