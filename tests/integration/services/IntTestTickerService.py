from unittest import TestCase

from config.logger_factory import logger_factory
from services import eod_data_service
from services.ExchangeType import ExchangeType
from services.RealtimeEquityPriceService import RealtimeEquityPriceService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.TickerService import TickerService

logger = logger_factory.create_logger(__name__)

class IntTestTickerService(TestCase):

  def test_merge(self):
    # Arrange
    df_equities = eod_data_service.get_shar_equity_data(SampleFileTypeSize.SMALL)
    df_tickers = TickerService.get_tickers_as_df()

    exchanges = df_tickers['exchange'].unique().tolist()
    logger.info(f"Exchanges: {exchanges}")

    # Act
    df = TickerService.merge_equities_with_eq_meta(df_equities, df_tickers)

    # Assert
    df_nasdaq = df[df['exchange'] == ExchangeType.NASDAQ]
    assert(df_nasdaq.shape[0] > 0)

    df_ibm = df[df['ticker'] == 'IBM']
    assert(df_ibm.iloc[0]['exchange'] == ExchangeType.NYSE)

  def test_foo(self):
    df_tickers = TickerService.get_tickers_as_df()

    # for c in ['exchange', 'sicsector', 'sicindustry', 'famasector', 'famaindustry', 'sector', 'industry']:
    #   things = df_tickers[c].unique().tolist()
    #   logger.info(f"{c}: {things}")

    tickers = df_tickers['ticker'].unique().tolist()
    tick_corrected = [t.replace("-", ".") for t in tickers]
    rt_tickers = RealtimeEquityPriceService.get_recent_ticker_list()

    rt_tick_corrected = [t.replace("-", ".") for t in rt_tickers]

    combined_missing = list(set(tick_corrected).difference(rt_tick_corrected)) + list(set(rt_tick_corrected).difference(tick_corrected))

    logger.info(f"{len(combined_missing)}; tickers: {combined_missing}")

    logger.info(f"Total quandl tickers: {len(tickers)}")
    logger.info(f"Total wtd tickers: {len(rt_tickers)}")



