import os
import random
from unittest import TestCase

import config
from config import logger_factory
from services import file_services
from services.AutoMlGeneralService import AutoMlGeneralService
from services.AutoMlPredictionService import AutoMlPredictionService

logger = logger_factory.create_logger(__name__)

class TestAutoMlPredictionService(TestCase):
  project_id = 'fletch22-ai'
  region_location = 'us-central1'

  def test_create_dataset(self):
    # Arrange
    dataset_name = "test123"
    auto_ml_service = AutoMlGeneralService(self.project_id, self.region_location)

    auto_ml_service.create_dataset(dataset_name)

    assert(auto_ml_service is not None)

  def test_import_data(self):
    # gs://api_uploads/process_2019-07-28_16-43-07-519.98_train_test_for_upload.zip
    dataset_name = "test123"
    auto_ml_service = AutoMlGeneralService(self.project_id, self.region_location)
    cloud_file_path = "process_2019-07-28_16-43-07-519.98_train_test_for_upload.zip"

    result = auto_ml_service.import_data(dataset_name, config.constants.GCS_UPLOAD_BUCKET_NAME, cloud_file_path)

    logger.info(f"Result: {result}")

  def test_sort_by_date_in_basename(self):
    # Arrange
    files = [
      'foo/0_ABC_2019-02-01.png',
      'foo/0_ABC_2019-01-01.png',
      'foo/1_ABC_2019-04-01.png',
      'foo/1_ABC_2019-04-02.png',
      'foo/0_ABC_2019-03-01.png'
    ]

    # Act
    files_sorted = AutoMlGeneralService.sort_by_date_in_basename(files)

    # Assert
    assert (files_sorted[0] == 'foo/0_ABC_2019-01-01.png')
    assert (files_sorted[1] == 'foo/0_ABC_2019-02-01.png')
    assert (files_sorted[2] == 'foo/0_ABC_2019-03-01.png')
    assert (files_sorted[3] == 'foo/1_ABC_2019-04-01.png')
    assert(files_sorted[-1] == 'foo/1_ABC_2019-04-02.png')


