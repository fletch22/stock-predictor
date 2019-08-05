import os
import shutil
from unittest import TestCase

import config
from config import logger_factory
from services import file_services
from services.CloudFileService import CloudFileService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.SelectChartZipUploadService import SelectChartZipUploadService

logger = logger_factory.create_logger(__name__)

class TestSelectChartZipUploadService(TestCase):
  files_to_delete = []
  cloud_files_to_delete = []
  cloud_file_service = CloudFileService()

  def tearDown(self):
    for d in self.files_to_delete:
      shutil.rmtree(d)

    for f in self.cloud_files_to_delete:
      self.cloud_file_service.delete_file_from_uploads(f)

  def test_pred_files(self):
    # Arrange
    prediction_dir = file_services.create_unique_folder(config.constants.CACHE_DIR, "unit_test")
    file_list = [
      "0_ABC_test.txt",
      "0_DEF_test.txt",
      "0_GHI_test.txt",
      "1_DEF_test.txt",
      "1_GHI_test.txt",
      "1_JKL_test.txt",
    ]
    graphed_dir = os.path.join(prediction_dir, "graphed")
    os.makedirs(graphed_dir)

    for f in file_list:
      file_path = os.path.join(graphed_dir, f)
      with open(file_path, "w+") as f:
        f.write("This is a test.")

    # Act
    SelectChartZipUploadService.prep_for_upload(prediction_dir, 4)

    # Assert
    file_0_0 = os.path.join(prediction_dir, "test_holdout", "0", "0.txt")
    file_1_1 = os.path.join(prediction_dir, "test_holdout", "1", "1.txt")
    assert(os.path.exists(file_0_0))
    assert(os.path.exists(file_1_1))

    train_test_file_0_0 = os.path.join(prediction_dir, "train_test", "0", "0.txt")
    assert (os.path.exists(train_test_file_0_0))

  def test_select_chart_zip_upload(self):
    # prediction_dir = os.path.join(config.constants.CACHE_DIR, "prediction_test")
    # test_dir = SelectAndChartService.prep_for_upload(prediction_dir, 4)

    # Arrange
    trading_days_span = 3

    # Act
    output_dir, cloud_dest_path = SelectChartZipUploadService.process(50, trading_days_span=trading_days_span, hide_image_details=False, sample_file_size=SampleFileTypeSize.SMALL)
    self.cloud_files_to_delete.append(cloud_dest_path)

    logger.info(f"Output dir: {output_dir}")

    # Assert
    assert(os.path.exists(output_dir))
    assert(self.cloud_file_service.file_exists_in_uploads(cloud_dest_path))

  def test_select_and_chart(self):
    min_price = 5.0
    amount_to_spend = 25000
    trading_days_span = 1000
    min_samples = 280000
    pct_gain_sought = 1.0

    package_path = SelectChartZipUploadService.select_and_process(min_price, amount_to_spend, trading_days_span, min_samples, pct_gain_sought)

    SelectChartZipUploadService.split_files_and_prep(min_samples, package_path, pct_test_holdout=10)



