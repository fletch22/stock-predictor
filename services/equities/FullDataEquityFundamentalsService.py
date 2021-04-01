import os
from datetime import datetime, timedelta
from typing import Sequence, Dict

import pandas as pd

import config
from config import logger_factory
from services.equities.EquityFundaDimension import EquityFundaDimension
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class FullDataEquityFundamentalsService():
  df = None

  def __init__(self):
    file_path = os.path.join(config.constants.SHAR_CORE_FUNDAMENTALS)
    self.df = pd.read_csv(file_path)

  def filter(self, symbols: Sequence[str]):
    symbols = self.prep_symbols(symbols)

    return self.df[(self.df['ticker'].isin(symbols))]

  def prep_symbols(self, symbols: Sequence[str]):
    symbols = [s.upper() for s in symbols]

    if "goog" in symbols:
      symbols.append("googl")

    return symbols

  def get_min_max_pe(self):
    return self.df['pe'].min(), self.df['pe'].max()

  def get_fundamentals_at_point_in_time(self, symbol, end_date: datetime, fundamental_indicator: Sequence[str]):

    # use groupby filter to collect each symbol's most recent MRQ pe value in list.
    # Set aside fundamental value
    # Do min max on list.
    # return as dictionary of fundamentals

    symbol = self.prep_symbols([symbol])[0]

    df = self.df

    begin_date = end_date - timedelta(days=(4.3 * 7 * 3)) # 90 days/1 Quarter

    end_date_str = date_utils.get_standard_ymd_format(end_date)
    begin_date_str = date_utils.get_standard_ymd_format(begin_date)

    df_fin_results = df[(df['datekey'] <= end_date_str) & (df['datekey'] >= begin_date_str)]

    df_fin_filtered = df_fin_results[(df_fin_results['dimension'] == EquityFundaDimension.MostRecentQuarterly)]
    if df_fin_filtered.shape[0] == 0:
      df_fin_filtered = df_fin_results[(df_fin_results['dimension'] == EquityFundaDimension.MostRecentTrailingYear)]
      if df_fin_filtered.shape[0] == 0:
        df_fin_filtered = df_fin_results[(df_fin_results['dimension'] == EquityFundaDimension.MostRecentAnnual)]

    df_fin_filtered.sort_values(by=['datekey'], inplace=True)

    result = {}
    def filter(df_grouped):
      last_row = df_grouped.iloc[-1]

      for fi in fundamental_indicator:
        if fi not in result.keys():
          result[fi] = {
            "values": [],
            "symbol_value": None
          }
        result[fi]['values'].append(last_row[fi])

        if last_row['ticker'] == symbol:
          result[fi]['symbol_value'] = last_row[fi]

    df_fin_filtered.groupby("ticker").filter(lambda x: filter(x))

    return result



