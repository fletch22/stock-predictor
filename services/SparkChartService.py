import pandas as pd

import config
from charts.ChartMode import ChartMode
from charts.ChartType import ChartType
from config import logger_factory
from services import chart_service
from services.EquityUtilService import EquityUtilService
from services.StockService import StockService

logger = logger_factory.create_logger(__name__)


class SparkChartService():

  def process(self, stock_infos):
    symbol = stock_infos["symbol"]
    trading_days_span = stock_infos['trading_days_span']
    pct_gain_sought = stock_infos['pct_gain_sought']
    save_dir = stock_infos["save_dir"]
    start_date = stock_infos["start_date"]
    end_date = stock_infos["end_date"]
    chart_type = stock_infos["chart_type"]
    chart_mode = stock_infos['chart_mode']
    package_path = stock_infos["package_path"]

    config.constants.IS_IN_SPARK_CONTEXT = True
    config.constants.SPARK_LOGGING_PATH = package_path
    logger.info(f"Spark Processing {symbol}...")

    df = EquityUtilService.get_df_from_ticker_path(symbol, True)
    df_sorted = df.sort_values(by=['date'], inplace=False)
    df_date_filtered = StockService.filter_dataframe_by_date(df_sorted, start_date, end_date)

    self.save_data_as_chart(symbol=symbol, save_dir=save_dir, chart_type=chart_type,
                            df=df_date_filtered, stock_infos=stock_infos, trading_days_span=trading_days_span,
                            pct_gain_sought=pct_gain_sought, chart_mode=chart_mode)

  def save_data_as_chart(self, symbol: str, save_dir: str, chart_type: ChartType, chart_mode: ChartMode, df: pd.DataFrame, stock_infos: dict, trading_days_span: int, pct_gain_sought: float):
    if chart_type == ChartType.Neopolitan:
      offset_info = stock_infos["offset_info"]
      for offset in offset_info.keys():
        df_offset = df.tail(df.shape[0] - offset).head(trading_days_span)

        category, yield_date = EquityUtilService.calculate_category_and_yield_date(df_offset, pct_gain_sought, chart_mode)

        fundies = offset_info[offset]
        logger.info(f"Fundies: {fundies}")
        chart_service.save_decorated_chart_to_filename(df_offset, fundy=fundies, category=category, symbol=symbol, yield_date=yield_date,
                                                       save_dir=save_dir, translate_save_path_hdfs=False)
    elif chart_type == ChartType.Vanilla:
      offsets = stock_infos["offsets"]
      for offset in offsets:
        df_offset = df.tail(df.shape[0] - offset).head(trading_days_span)
        category, yield_date = EquityUtilService.calculate_category_and_yield_date(df_offset, pct_gain_sought, chart_mode)

        chart_service.save_data_as_chart(symbol=symbol, category=category, df=df_offset, yield_date=yield_date, save_dir=save_dir, translate_save_path_hdfs=True)
    else:
      raise Exception(f"Chart type {chart_type} is not recognized.")
