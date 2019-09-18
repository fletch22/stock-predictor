import os
import statistics
from datetime import datetime
from unittest import TestCase

from stopwatch import Stopwatch

import config
from config import logger_factory
from services import eod_data_service
from services.equities.FullDataEquityFundamentalsService import FullDataEquityFundamentalsService
from utils import date_utils
import pandas as pd
import numpy as np

logger = logger_factory.create_logger(__name__)

class TestFullDataEquityFundamentalsService(TestCase):

  def test_open_fundamentals(self):
    # Arrange
    efs = FullDataEquityFundamentalsService()

    # Act
    df = efs.df

    logger.info(f"Cols: {df.columns}")

    assert (df.shape[0] > 0)

  def test_get_fundies(self):
    # Arrange
    stopwatch = Stopwatch()
    efs = FullDataEquityFundamentalsService()

    efs.filter(['ibm', 'aapl', 'msft'])

    end_date = date_utils.parse_datestring('2019-01-23')

    # Act
    stopwatch.start()
    fundies = efs.get_fundamentals_at_point_in_time('googl', end_date, ['pe'])
    stopwatch.stop()

    logger.info(f"Fundies: {fundies}; elapsed: {stopwatch}")

  def test_get_average_volatility(self):
    end_date = date_utils.parse_datestring("2018-12-31")
    df = get_mean_stdev(end_date, 253)

    logger.info(f"Mean stdev: {statistics.mean(df['price'].values.tolist())}")

def get_mean_stdev(end_date: datetime, trading_days_span: int):
  end_date_str = date_utils.get_standard_ymd_format(end_date)
  df = eod_data_service.get_todays_merged_shar_data()
  df_dt_filtered = df[df['date'] <= end_date_str]
  agg_std = []

  def get_std(df, agg_std):
    if df.shape[0] > 1:
      df = df.iloc[-trading_days_span:, :]
      symbol = df.iloc[0]['ticker']
      std = statistics.stdev(df['close'].values.tolist())
      agg_std.append([symbol, std])
      logger.info(f"Calc stdev for symbol {symbol}: {std}")

  df_dt_filtered.groupby('ticker').filter(lambda x: get_std(x, agg_std))

  df_agg = pd.DataFrame(agg_std, columns=['ticker', 'price'])

  # NOTE: 2019-09-14: chris.flesche: giving a haircut to remove outlier.
  return df_agg[df_agg['price'] < np.percentile(df_agg['price'], 99)]
