import config
from charts.ChartType import ChartType
from config import logger_factory
from services import chart_service
from services.EquityUtilService import EquityUtilService
from services.StockService import StockService


class SparkChartService():

  def process(self, stock_infos):
    symbol = stock_infos["symbol"]
    trading_days_span = stock_infos['trading_days_span']
    pct_gain_sought = stock_infos['pct_gain_sought']
    save_dir = stock_infos["save_dir"]
    start_date = stock_infos["start_date"]
    end_date = stock_infos["end_date"]
    chart_type = stock_infos["chart_type"]
    package_path = stock_infos["package_path"]

    config.constants.IS_IN_SPARK_CONTEXT = True
    config.constants.SPARK_LOGGING_PATH = package_path
    self.slogger = logger_factory.create_logger(__name__, task_dir=package_path)
    self.slogger.info(f"Spark Processing {symbol}...")

    df = EquityUtilService.get_df_from_ticker_path(symbol, True)
    df_sorted = df.sort_values(by=['date'], inplace=False)

    df_date_filtered = StockService.filter_dataframe_by_date(df_sorted, start_date, end_date)

    if chart_type == ChartType.Neopolitan:
      offset_info = stock_infos["offset_info"]
      for offset in offset_info.keys():
        df_offset = df_date_filtered.tail(df_date_filtered.shape[0] - offset).head(trading_days_span)
        category, yield_date_str = EquityUtilService.calculate_category(df_offset, pct_gain_sought)

        fundies = offset_info[offset]
        self.slogger.info(f"Fundies: {fundies}")
        chart_service.save_decorated_chart_to_filename(df_offset, fundy=fundies, category=category, symbol=symbol, yield_date_str=yield_date_str,
                                                       save_dir=save_dir, translate_save_path_hdfs=False)
    elif chart_type == ChartType.Vanilla:
      offsets = stock_infos["offsets"]
      for offset in offsets:
        df_offset = df_date_filtered.tail(df_date_filtered.shape[0] - offset).head(trading_days_span)
        category, yield_date_str = EquityUtilService.calculate_category(df_offset, pct_gain_sought)

        chart_service.save_data_as_chart(symbol=symbol, category=category, df=df_offset, yield_date_str=yield_date_str, save_dir=save_dir, translate_save_path_hdfs=True)
    else:
      raise Exception(f"Chart type {chart_type} is not recognized.")


