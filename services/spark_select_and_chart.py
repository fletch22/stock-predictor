from config import logger_factory
from services import chart_service
from services.Eod import Eod
from services.EquityUtilService import EquityUtilService
from services.StockService import StockService

logger = logger_factory.create_logger(__name__)

def spark_process_sample_info(symbol_info):
  symbol = symbol_info["symbol"]
  logger.info(f"Processing {symbol}...")
  offsets = symbol_info["offsets"]
  trading_days_span = symbol_info['trading_days_span']
  pct_gain_sought = symbol_info['pct_gain_sought']
  save_dir = symbol_info["save_dir"]
  start_date = symbol_info["start_date"]
  end_date = symbol_info["end_date"]

  df = EquityUtilService.get_df_from_ticker_path(symbol, True)
  df_sorted = df.sort_values(by=['date'], inplace=False)
  df_date_filtered = StockService.filter_dataframe_by_date(df_sorted, start_date, end_date)

  for start_offset in offsets:
    df_offset = df_date_filtered.tail(df_date_filtered.shape[0] - start_offset).head(trading_days_span)
    df_tailed = df_offset.tail(2)

    close_price = df_tailed.iloc[-2][Eod.CLOSE]
    high_price = df_tailed.iloc[-1]['high']
    yield_date_str = df_offset.iloc[-1]["date"]

    logger.info(f"Ticker: -2: {df_tailed.iloc[-2]['ticker']}; -1 {df_tailed.iloc[-2]['ticker']} BuyPrice: {close_price}; SellPrice: {high_price}; Bet date: {yield_date_str}")
    pct_gain = ((high_price - close_price) / close_price) * 100

    category = "1" if pct_gain >= pct_gain_sought else "0"

    chart_service.save_data_as_chart(symbol, category, df_offset, yield_date_str, save_dir, translate_save_path_hdfs=True)

