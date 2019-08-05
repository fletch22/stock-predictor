import os
import random
import shutil

import findspark
from pyspark import SparkContext

import config
from config import logger_factory
from services import chart_service, file_services
from services.CloudFileService import CloudFileService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService
from services.spark_select_and_chart import spark_process_sample_info

logger = logger_factory.create_logger(__name__)


class SelectChartZipUploadService:

  @classmethod
  def process(cls, sample_size: int, trading_days_span=1000, persist_data: bool = True, hide_image_details=True, sample_file_size: SampleFileTypeSize = SampleFileTypeSize.LARGE):
    par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", cls.__name__)
    package_path = file_services.create_unique_folder(par_dir, "process")
    # unique_name = os.path.split(package_path)[1]

    df_good, df_bad = StockService.get_sample_data(package_path, min_samples=sample_size, trading_days_span=trading_days_span, sample_file_size=sample_file_size, persist_data=persist_data)

    graph_dir = os.path.join(package_path, "graphed")
    os.makedirs(graph_dir, exist_ok=True)

    chart_service.plot_and_save(df_good, graph_dir, category="1", hide_details=hide_image_details)
    chart_service.plot_and_save(df_bad, graph_dir, category="0", hide_details=hide_image_details)

    num_files_needed = sample_size // 20
    if num_files_needed > 6000:
      num_files_needed = 6000
    train_test_dir, _ = cls.prep_for_upload(package_path, num_files_needed)

    # output_zip_path = os.path.join(package_path, f"{unique_name}_train_test_for_upload.zip")
    # file_services.zip_dir(train_test_dir, output_zip_path)
    #
    # cloud_dest_path = cls.upload_file(output_zip_path)

    return package_path #, cloud_dest_path

  @classmethod
  def upload_file(cls, file_path: str) -> str:
    cloud_file_services = CloudFileService()
    parent_dir = os.path.dirname(file_path)
    dest_file_path = file_path.replace(f"{parent_dir}{os.path.sep}", "")
    cloud_file_services.upload_file(file_path, dest_file_path)

    return dest_file_path

  @classmethod
  def prep_for_upload(cls, prediction_dir: str, num_files_needed: int):
    parent_dir = os.path.join(prediction_dir, "graphed")

    categories = ["1", "0"]
    files_needed = num_files_needed // 2
    logger.info(f"files needed {files_needed}; parent_dir: {parent_dir}")

    test_dir = os.path.join(prediction_dir, "test_holdout")
    os.makedirs(test_dir, exist_ok=True)

    train_test_dir = os.path.join(prediction_dir, "train_test")
    os.makedirs(train_test_dir, exist_ok=True)

    def move(file_list, cat_dir, hide_details: bool):
      for ndx, f in enumerate(file_list):
        _, file_extension = os.path.splitext(f)

        if hide_details:
          filename = f"{ndx}{file_extension}"
        else:
          filename = os.path.basename(f)

        dest_path = os.path.join(cat_dir, filename)
        logger.info(f"Moving {f} to {dest_path}")
        shutil.move(f, dest_path)

    for cat in categories:
      files_raw = file_services.walk(parent_dir)
      files_filtered = [f for f in files_raw if os.path.basename(f).startswith(f"{cat}_")]
      random.shuffle(files_filtered, random.random)
      test_holdout_files = files_filtered[:files_needed]
      train_test_files = files_filtered[files_needed:]

      cat_dir_test = os.path.join(test_dir, cat)
      os.makedirs(cat_dir_test, exist_ok=True)

      cat_dir_train = os.path.join(train_test_dir, cat)
      os.makedirs(cat_dir_train, exist_ok=True)

      move(test_holdout_files, cat_dir_test, False)
      move(train_test_files, cat_dir_train, True)

    return train_test_dir, test_dir

  @classmethod
  def select_and_process(cls, min_price: float, amount_to_spend: float, trading_days_span: int, min_samples: int, pct_gain_sought: float):
    par_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", cls.__name__)
    package_path = file_services.create_unique_folder(par_dir, "process")

    graph_dir = os.path.join(package_path, "graphed")
    os.makedirs(graph_dir, exist_ok=True)

    output_dir = os.path.join(config.constants.CACHE_DIR, "spark_test")
    os.makedirs(output_dir, exist_ok=True)
    df_g_filtered = StockService._get_and_prep_equity_data(amount_to_spend, trading_days_span, min_price, SampleFileTypeSize.LARGE)

    logger.info(f"Num with symbols after group filtering: {df_g_filtered.shape[0]}")

    sample_info = StockService.get_sample_infos(df_g_filtered, trading_days_span, min_samples)

    findspark.init()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")
    print(sc._jsc.sc().uiWebUrl().get())

    symbol_arr = []
    for symbol in sample_info.keys():
      s_dict = sample_info[symbol]
      s_dict['symbol'] = symbol
      s_dict['trading_days_span'] = trading_days_span
      s_dict['pct_gain_sought'] = pct_gain_sought
      s_dict['save_dir'] = graph_dir
      symbol_arr.append(s_dict)

    rdd = sc.parallelize(symbol_arr)

    rdd.foreach(spark_process_sample_info)

    sc.stop()

    return package_path

  @classmethod
  def split_files_and_prep(cls, sample_size: int, package_path: str, pct_test_holdout: float=20):
    num_files_needed: int = int(sample_size * (pct_test_holdout/100))
    train_test_dir, _ = cls.prep_for_upload(package_path, num_files_needed)