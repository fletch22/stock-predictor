from datetime import datetime

from services.EquityUtilService import EquityUtilService
from services.StockService import StockService
from utils import date_utils


class EquityToLegacyDataConversionService:

  @classmethod
  def get_symbol(cls, symbol: str, num_records: int = None, latest_date: datetime = None):
    df = StockService.get_symbol_df(symbol, translate_file_path_to_hdfs=False)
    # df = EquityUtilService.get_df_from_ticker_path(symbol, False)

    if latest_date is not None:
      df = df[df['date'] <= date_utils.get_standard_ymd_format(latest_date)]

    df.sort_values(by='date', inplace=True)

    if num_records is not None:
      df = df.iloc[-num_records:]

    df = df.rename(columns={'date': 'Date', 'close': 'Adj Close', 'closeunadj': 'Close', 'open': 'Open', 'high': 'High', 'low': 'Low', 'volume': 'Volume'})
    df = df.drop(columns=['ticker', 'lastupdated', 'dividends'])
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]

    return df
