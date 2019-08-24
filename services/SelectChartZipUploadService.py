import os
from typing import Dict

import findspark
from datetime import datetime
from pyspark import SparkContext

import config
from config import logger_factory
from services import file_services
from services.AutoMlGeneralService import AutoMlGeneralService
from services.CloudFileService import CloudFileService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService
from services.equities import equity_fundamentals_service
from services.equities.FullDataEquityFundamentalsService import FullDataEquityFundamentalsService
from services.equities.FundyMinMax import FundyMinMax
from services.spark import spark_select_and_chart
from services.spark.spark_select_and_chart import spark_process

logger = logger_factory.create_logger(__name__)


class SelectChartZipUploadService:

  # @classmethod
  # def process(cls, sample_size: int, start_date: datetime, end_date: datetime, trading_days_span=1000, persist_data: bool = True, hide_image_details=True, sample_file_size: SampleFileTypeSize = SampleFileTypeSize.LARGE):
  #   par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", cls.__name__)
  #   package_path = file_services.create_unique_folder(par_dir, "process")
  #   # unique_name = os.path.split(package_path)[1]
  #
  #   df_good, df_bad = StockService.get_sample_data(package_path, min_samples=sample_size, start_date=start_date, end_date=end_date,
  #                                                  trading_days_span=trading_days_span, sample_file_size=sample_file_size, persist_data=persist_data)
  #
  #   graph_dir = os.path.join(package_path, "graphed")
  #   os.makedirs(graph_dir, exist_ok=True)
  #
  #   chart_service.plot_and_save_for_learning(df_good, graph_dir, category="1")
  #   chart_service.plot_and_save_for_learning(df_bad, graph_dir, category="0")
  #
  #   num_files_needed = sample_size // 20
  #   if num_files_needed > 6000:
  #     num_files_needed = 6000
  #   train_test_dir, _ = cls.prep_for_upload(package_path, num_files_needed)
  #
  #   # output_zip_path = os.path.join(package_path, f"{unique_name}_train_test_for_upload.zip")
  #   # file_services.zip_dir(train_test_dir, output_zip_path)
  #   #
  #   # cloud_dest_path = cls.upload_file(output_zip_path)
  #
  #   return package_path #, cloud_dest_path

  @classmethod
  def upload_file(cls, file_path: str) -> str:
    cloud_file_services = CloudFileService()
    parent_dir = os.path.dirname(file_path)
    dest_file_path = file_path.replace(f"{parent_dir}{os.path.sep}", "")
    cloud_file_services.upload_file(file_path, dest_file_path)

    return dest_file_path

  @classmethod
  def select_and_process(cls, min_price: float, amount_to_spend: float, trading_days_span: int, min_samples: int, pct_gain_sought: float, start_date: datetime, end_date: datetime):
    par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", cls.__name__)
    package_path = file_services.create_unique_folder(par_dir, "process")

    graph_dir = os.path.join(package_path, "graphed")
    os.makedirs(graph_dir, exist_ok=True)

    output_dir = os.path.join(config.constants.CACHE_DIR, "spark_test")
    os.makedirs(output_dir, exist_ok=True)
    df_g_filtered = StockService._get_and_prep_equity_data(amount_to_spend, trading_days_span, min_price, SampleFileTypeSize.LARGE, start_date, end_date)

    logger.info(f"Num with symbols after group filtering: {df_g_filtered.shape[0]}")

    sample_infos = StockService.get_sample_infos(df_g_filtered, trading_days_span, min_samples, False, start_date, end_date)

    stock_infos = equity_fundamentals_service.get_scaled_sample_infos(sample_infos, trading_days_span, start_date, end_date, desired_fundamentals=['pe', 'ev', 'eps'])

    for sinfo in stock_infos:
      sinfo['trading_days_span'] = trading_days_span
      sinfo['pct_gain_sought'] = pct_gain_sought
      sinfo['save_dir'] = graph_dir
      sinfo['start_date'] = start_date
      sinfo['end_date'] = end_date

    spark_select_and_chart.do_spark(stock_infos)

    return package_path

  @classmethod
  def split_files_and_prep(cls, sample_size: int, package_path: str, pct_test_holdout: float=20):
    num_files_needed: int = int(sample_size * (pct_test_holdout/100))
    logger.info(f"Setting aside {num_files_needed} for holdout.")
    train_test_dir, _ = AutoMlGeneralService.prep_for_upload(package_path, num_files_needed)

    return train_test_dir

  @classmethod
  def create_learning_set(cls, start_date: datetime, end_date: datetime, min_samples:int, pct_gain_sought: float, trading_days_span: int, pct_test_holdout: float, min_price: float, amount_to_spend: float):
    package_path = cls.select_and_process(min_price, amount_to_spend, trading_days_span, min_samples, pct_gain_sought, start_date, end_date)

    return SelectChartZipUploadService.split_files_and_prep(min_samples, package_path, pct_test_holdout)

