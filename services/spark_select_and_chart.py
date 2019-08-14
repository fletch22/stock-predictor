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

    category, yield_date_str = EquityUtilService.calculate_category(df_offset, pct_gain_sought)

    chart_service.save_data_as_chart(symbol, category, df_offset, yield_date_str, save_dir, translate_save_path_hdfs=True)

