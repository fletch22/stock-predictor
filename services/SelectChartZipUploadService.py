import os

from datetime import datetime, timedelta

import config
from charts.ChartType import ChartType
from config import logger_factory
from learning_set.LearningSetMetaData import LearningSetMetaData
from services import file_services
from services.AutoMlGeneralService import AutoMlGeneralService
from services.CloudFileService import CloudFileService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService
from services.equities import equity_fundamentals_service
from services.spark import spark_select_and_chart, spark_get_fundamentals

logger = logger_factory.create_logger(__name__)


class SelectChartZipUploadService:

  @classmethod
  def upload_file(cls, file_path: str) -> str:
    cloud_file_services = CloudFileService()
    parent_dir = os.path.dirname(file_path)
    dest_file_path = file_path.replace(f"{parent_dir}{os.path.sep}", "")
    cloud_file_services.upload_file(file_path, dest_file_path)

    return dest_file_path

  @classmethod
  def select_and_process_one_day(cls, min_price: float, amount_to_spend: float, trading_days_span: int, min_samples: int,
                                 pct_gain_sought: float, yield_date: datetime, chart_type: ChartType, volatility_min: float):
    par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", cls.__name__)
    package_path = file_services.create_unique_folder(par_dir, "process")

    graph_dir = os.path.join(package_path, "graphed")
    os.makedirs(graph_dir, exist_ok=True)

    df_g_filtered = StockService.get_and_prep_equity_data_one_day(amount_to_spend, trading_days_span, min_price, yield_date, volatility_min=volatility_min)

    if df_g_filtered.shape[0] > 0:
      logger.info(f"Num rows with symbols after group filtering: {df_g_filtered.shape[0]}")

      start_date = yield_date - timedelta(days=trading_days_span * 2)
      end_date = yield_date

      sample_infos = StockService.get_sample_infos_one_day(df_g_filtered, trading_days_span, min_samples, start_date, yield_date, False)

      logger.info(f"si: {type(sample_infos)}")

      if chart_type == ChartType.Neopolitan:
        stock_infos = equity_fundamentals_service.get_scaled_sample_infos(sample_infos, package_path, trading_days_span, start_date=start_date, end_date=end_date, desired_fundamentals=['pe', 'ev', 'eps'])
      elif chart_type == ChartType.Vanilla:
        stock_infos = spark_get_fundamentals.convert_to_spark_array(sample_infos, trading_days_span, start_date, end_date, desired_fundamentals=None)
      else:
        raise Exception(f"Unable to process chart_type {chart_type}")

      for sinfo in stock_infos:
        sinfo['trading_days_span'] = trading_days_span
        sinfo['pct_gain_sought'] = pct_gain_sought
        sinfo['save_dir'] = graph_dir
        sinfo['start_date'] = start_date
        sinfo['end_date'] = end_date
        sinfo['chart_type'] = chart_type
        sinfo['package_path'] = package_path

      spark_select_and_chart.do_spark(stock_infos)

    return df_g_filtered, package_path

  @classmethod
  def select_and_process(cls, lsm: LearningSetMetaData):
    package_path, graph_dir = cls.create_package(lsm)

    df_g_filtered = StockService.get_and_prep_equity_data(lsm.amount_to_spend, lsm.trading_days_span, lsm.min_price, lsm.volatility_min, lsm.start_date, lsm.end_date)

    logger.info(f"Num with symbols after group filtering: {df_g_filtered.shape[0]}")

    stock_infos = StockService.get_sample_infos(df_g_filtered, lsm.trading_days_span, lsm.min_samples, False, lsm.start_date, lsm.end_date)
    if ChartType.Neopolitan == lsm.chart_type:
      stock_infos = equity_fundamentals_service.get_scaled_sample_infos(stock_infos, package_path, lsm.trading_days_span, lsm.start_date, lsm.end_date, desired_fundamentals=['pe', 'ev', 'eps'])

    for sinfo in stock_infos:
      sinfo['trading_days_span'] = lsm.trading_days_span
      sinfo['pct_gain_sought'] = lsm.pct_gain_sought
      sinfo['save_dir'] = graph_dir
      sinfo['start_date'] = lsm.start_date
      sinfo['end_date'] = lsm.end_date
      sinfo['chart_type'] = lsm.chart_type
      sinfo['package_path'] = package_path

    spark_select_and_chart.do_spark(stock_infos)

    return package_path

  @classmethod
  def create_package(cls, lsm: LearningSetMetaData):
    par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", cls.__name__)
    package_path = file_services.create_unique_folder(par_dir, "process")

    graph_dir = os.path.join(package_path, "graphed")
    os.makedirs(graph_dir, exist_ok=True)

    output_dir = os.path.join(config.constants.CACHE_DIR, "spark_test")
    os.makedirs(output_dir, exist_ok=True)

    metadata_dir = os.path.join(package_path, 'metadata')
    os.makedirs(metadata_dir, exist_ok=True)

    lsm_path = os.path.join(metadata_dir, LearningSetMetaData.pickle_filename())
    lsm.persist(lsm_path)

    return package_path, graph_dir

  @classmethod
  def create_learning_set(cls, lsm: LearningSetMetaData):
    package_path = cls.select_and_process(lsm)

    return SelectChartZipUploadService.split_files_and_prep(lsm.min_samples, package_path, lsm.pct_test_holdout)

  @classmethod
  def split_files_and_prep(cls, sample_size: int, package_path: str, pct_test_holdout: float=20):
    num_files_needed: int = int(sample_size * (pct_test_holdout/100))
    logger.info(f"Setting aside {num_files_needed} for holdout.")
    train_test_dir, _ = AutoMlGeneralService.prep_for_upload(package_path, num_files_needed)

    return train_test_dir


