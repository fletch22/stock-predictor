import os
import random
from unittest import TestCase

import config
from config import logger_factory
from services import file_services
from services.AutoMlPredictionService import AutoMlPredictionService

logger = logger_factory.create_logger(__name__)

class TestAutoMlPredictionService(TestCase):

  def test_predict_and_calculate(self):
    # Arrange
    # short_model_id = "ICN1033531528117526290" # fixed 63 2%
    # short_model_id = "ICN7463116937945670174"  # 68% 1000%
    # short_model_id = "ICN8859340869188572136"  # 65% 100K%
    # short_model_id = "ICN4970054642680386601" # big 240 corrected 1
    short_model_id = "ICN3581216182088743188" # big 240 corrected 2
    # package_folder = "process_2019-08-04_09-08-53-876.24"
    package_folder = "process_2019-08-04_18-23-16-913.85"
    data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=.60)
    image_dir = os.path.join(data_cache_dir, "test_holdout")

    # Act
    auto_ml_service.predict_and_calculate(image_dir, max_files=10000)

    # Assert
    assert(True)




