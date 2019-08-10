import os
import shutil
from unittest import TestCase

import findspark
import math
from ipython_genutils.py3compat import xrange
from pyspark import SparkContext, SparkFiles

import config
from config.logger_factory import logger_factory
from services import file_services
from services.CloudFileService import CloudFileService

logger = logger_factory.create_logger(__name__)

class TestCloudFileService(TestCase):
  delete_dirs = []
  delete_cloud_files = []
  cloud_file_service = None

  def setUp(self) -> None:
    self.cloud_file_service = CloudFileService()

  def tearDown(self) -> None:
    for d in self.delete_dirs:
      shutil.rmtree(d)

    for d in self.delete_cloud_files:
      self.cloud_file_service.delete_file_from_uploads(d)

  def test_file_upload(self):
    # Arrange
    output_dir = file_services.create_unique_folder(config.constants.OUTPUT_DIR, "unit_test")

    file_path = os.path.join(output_dir, "test.txt")
    with open(file_path, 'x') as f:
      f.write("This is a test.")

    cloud_file_path = f"uploaded/foo/{os.path.basename(file_path)}"
    self.delete_cloud_files.append(cloud_file_path)

    # Act
    self.cloud_file_service.upload_file(file_path, cloud_file_path)

    # Assert
    assert(self.cloud_file_service.file_exists_in_uploads(cloud_file_path))

  def test_list(self):
    # Arrange

    # Act
    files = self.cloud_file_service.list_filenames("")

    # Assert
    assert(len(files) > 0)

  def test_folder_upload(self):
    # Arrange
    from pathlib import Path
    package_folder = "process_2019-08-07_08-27-02-182.53"
    project_dir = os.path.join(config.constants.FINANCE_DATA_DIR, "test_files", package_folder, "train_test")
    max_files = 60

    cloud_file_service = CloudFileService()

    cloud_file_service.upload_multi(project_dir, package_folder, max_files)

  def test_foo(self):
    # Arrange
    from pathlib import Path
    package_folder = "process_2019-08-06_22-46-56-230.65"
    project_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder, "train_test")

    cloud_file_service = CloudFileService()

    cloud_file_service.upload_multi(project_dir, package_folder)
