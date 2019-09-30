import os
from datetime import timedelta

import config
from charts.ChartMode import ChartMode
from charts.ChartType import ChartType
from config import logger_factory
from learning_set.LearningSetMetaData import LearningSetMetaData
from prediction.PredictionRosebud import PredictionRosebud
from services import file_services
from services.AutoMlGeneralService import AutoMlGeneralService
from services.CloudFileService import CloudFileService
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
  def select_and_process_one_day(cls, prediction_rosebud: PredictionRosebud):
    par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", cls.__name__)
    package_path = file_services.create_unique_folder(par_dir, "process")

    graph_dir = os.path.join(package_path, "graphed")
    os.makedirs(graph_dir, exist_ok=True)

    df_g_filtered = StockService.get_and_prep_equity_data_one_day(prediction_rosebud=prediction_rosebud)

    trading_days_span = prediction_rosebud.num_days_to_sample
    yield_date = prediction_rosebud.yield_date
    min_samples = prediction_rosebud.max_files
    chart_type = prediction_rosebud.chart_type
    chart_mode = prediction_rosebud.chart_mode

    if df_g_filtered.shape[0] > 0:
      logger.info(f"Num rows with symbols after group filtering: {df_g_filtered.shape[0]}")

      start_date = yield_date - timedelta(days=trading_days_span * 2)
      end_date = yield_date

      sample_infos = StockService.get_sample_infos_one_day(df_g_filtered=df_g_filtered, num_days_avail=trading_days_span, min_samples=min_samples, start_date=start_date, yield_date=yield_date, translate_file_path_to_hdfs=False)

      logger.info(f"si: {type(sample_infos)}")

      if chart_type == ChartType.Neopolitan:
        stock_infos = equity_fundamentals_service.get_scaled_sample_infos(sample_infos=sample_infos, package_path=package_path, trading_days_span=trading_days_span, start_date=start_date, end_date=end_date, desired_fundamentals=['pe', 'ev', 'eps'])
      elif chart_type == ChartType.Vanilla:
        stock_infos = spark_get_fundamentals.convert_to_spark_array(sample_infos=sample_infos, trading_days_span=trading_days_span, start_date=start_date, end_date=end_date, desired_fundamentals=None)
      else:
        raise Exception(f"Unable to process chart_type '{chart_type}'.")

      for sinfo in stock_infos:
        sinfo['trading_days_span'] = trading_days_span
        sinfo['pct_gain_sought'] = prediction_rosebud.pct_gain_sought
        sinfo['save_dir'] = graph_dir
        sinfo['start_date'] = start_date
        sinfo['end_date'] = end_date
        sinfo['chart_type'] = chart_type
        sinfo['package_path'] = package_path
        sinfo['chart_mode'] = chart_mode

      spark_select_and_chart.do_spark(stock_infos=stock_infos)

    return df_g_filtered, package_path, graph_dir

  @classmethod
  def select_and_process(cls, lsm: LearningSetMetaData):
    package_path, graph_dir = cls.create_package(lsm)

    df_g_filtered = StockService.get_and_prep_equity_data(lsm.amount_to_spend, lsm.trading_days_span, lsm.min_price, lsm.volatility_min, lsm.start_date, lsm.end_date)

    logger.info(f"Num with symbols after group filtering: {df_g_filtered.shape[0]}")

    if df_g_filtered.shape[0] == 0:
      raise Exception("Could not find any symbols for analysis. Filtered out all candidates?")

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
      sinfo['chart_mode'] = ChartMode.BackTest

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
  def split_files_and_prep(cls, sample_size: int, package_path: str, pct_test_holdout: float = 20):
    num_files_needed: int = int(sample_size * (pct_test_holdout / 100))
    logger.info(f"Setting aside {num_files_needed} for holdout.")
    train_test_dir, _ = AutoMlGeneralService.prep_for_upload(package_path, num_files_needed)

    return train_test_dir
