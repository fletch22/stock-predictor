import os
import random
from unittest import TestCase

from stopwatch import Stopwatch

import config
from config import logger_factory
from services import file_services
from services.AutoMlPredictionService import AutoMlPredictionService

logger = logger_factory.create_logger(__name__)

class TestAutoMlPredictionService(TestCase):

  def test_predict_and_calculate(self):
    # Arrange
    short_model_id = "ICN7780367212284440071" # fut_prof 68%
    # short_model_id = "ICN5723877521014662275" # closeunadj
    # package_folder = "process_2019-08-04_09-08-53-876.24"
    # package_folder = "process_2019-08-06_21-07-03-123.23"
    # package_folder = "process_2019-08-07_08-27-02-182.53"
    # package_folder = "process_2019-08-07_21-01-14-275.25"
    # package_folder = "process_2019-08-08_21-28-43-496.67"
    # data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
    # image_dir = os.path.join(data_cache_dir, "test_holdout")

    package_folder = ""  # 2019-07-23 .56 ->
    # package_folder = "tsczuz__2019-08-13_07-44-49-19.99"  # 2019-07-17 .56 -> -.36
    # package_folder = "tsczuz__2019-08-13_08-02-50-934.15"  # 2019-07-18 .56 -> -.07
    # package_folder = "tsczuz__2019-08-13_23-05-25-314.13"  # 2019-07-19 .56 -> .1
    # package_folder = "tsczuz__2019-08-13_08-19-29-765.36"  # 2019-07-22 .56 -> .14
    # package_folder = "tsczuz__2019-08-13_08-34-11-93.47"  # 2019-07-23 .56 -> .39
    # package_folder = "tsczuz__2019-08-13_19-33-09-981.39"  # 2019-07-24 .56 -> .48
    # package_folder = "tsczuz__2019-08-13_19-50-20-163.87"  # 2019-07-25 .56 -> -.09
    # package_folder = ""  # 2019-07-26 .56 ->
    # package_folder = "tsczuz__2019-08-13_20-15-02-219.27"  # 2019-07-29 .56 -> -.18
    # package_folder = "tsczuz__2019-08-13_20-32-05-80.24" #2019-07-30 .56 -> .33
    # package_folder = "tsczuz__2019-08-13_21-05-59-524.71"  # 2019-07-31 .56 -> .31
    # package_folder = "tsczuz__2019-08-13_21-16-52-909.21" # 2019-08-01 .56 -> .08

    data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "test_one_day")
    image_dir = os.path.join(data_cache_dir, package_folder)
    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=.56)
    sought_gain_frac = .01

    # Act
    stopwatch = Stopwatch()
    stopwatch.start()
    auto_ml_service.predict_and_calculate(image_dir, sought_gain_frac, max_files=3000, purge_cached=False)
    stopwatch.stop()

    logger.info(f"Elapsed time: {round(stopwatch.duration/60, 2)} minutes")

    # Assert
    assert(True)




