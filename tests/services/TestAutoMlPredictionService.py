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
    # import os
    # os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf \"spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com -Dhttp.proxyPort=1234 -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1\" pyspark-shell"
    # os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local[2] pyspark-shell"

    # Arrange
    # short_model_id = "ICN7780367212284440071" # fut_prof 68%
    # short_model_id = "ICN5854415975519476169" # order_corr 64%
    # short_model_id = "ICN7516754052793253430" # order_cor 64.5 Round II - broken
    # short_model_id = "ICN1518571776858868271" # order_corr 64 fixed
    # short_model_id = "ICN5273088814455939732"  # order_corr 64 fixed II
    short_model_id = "ICN5723877521014662275" # closeunadj
    # package_folder = "process_2019-08-04_09-08-53-876.24"
    # package_folder = "process_2019-08-06_21-07-03-123.23"
    # package_folder = "process_2019-08-07_08-27-02-182.53"
    # package_folder = "process_2019-08-07_21-01-14-275.25"
    package_folder = "process_2019-08-08_21-28-43-496.67"
    data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=.50)
    image_dir = os.path.join(data_cache_dir, "test_holdout")
    sought_gain_frac = .09

    # Act
    stopwatch = Stopwatch()
    stopwatch.start()
    auto_ml_service.predict_and_calculate(image_dir, sought_gain_frac, max_files=2500)
    stopwatch.stop()

    logger.info(f"Elapsed time: {round(stopwatch.duration/60, 2)} minutes")

    # Assert
    assert(True)




