import math
import os
import shutil
from typing import List
from unittest import TestCase

import config
from categorical.BinaryCategoryType import BinaryCategoryType
from config.logger_factory import logger_factory
from services import file_services, project_upload_service
from services.CloudFileService import CloudFileService
from utils import date_utils
from datetime import datetime
import pandas as pd

logger = logger_factory.create_logger(__name__)


class TestCloudFileService(TestCase):
  delete_dirs = []
  delete_cloud_files = []
  cloud_file_service = None

  def setUp(self) -> None:
    self.cloud_file_service = CloudFileService()

  def tearDown(self) -> None:
    # for d in self.delete_dirs:
    #   shutil.rmtree(d)

    for d in self.delete_cloud_files:
      self.cloud_file_service.delete_file_from_uploads(d)

  def test_file_upload(self):
    # Arrange
    output_dir = file_services.create_unique_folder(config.constants.OUTPUT_DIR, "unit_test")
    self.delete_dirs.append(output_dir)

    file_path = os.path.join(output_dir, "test.txt")
    with open(file_path, 'x') as f:
      f.write("This is a test.")

    cloud_file_path = f"uploaded/foo/{os.path.basename(file_path)}"
    self.delete_cloud_files.append(cloud_file_path)

    # Act
    self.cloud_file_service.upload_file(file_path, cloud_file_path)

    # Assert
    assert (self.cloud_file_service.file_exists_in_uploads(cloud_file_path))

  def create_test_file(self, output_dir: str, filename: str):
    file_path = os.path.join(output_dir, filename)
    with open(file_path, 'x') as f:
      f.write(f"This is a test for {file_path}.")

    return file_path

  def test_package_upload(self):
    # Arrange
    package_folder = file_services.create_unique_folder(config.constants.OUTPUT_DIR, "unit_test")
    self.delete_dirs.append(package_folder)

    train_test_dir = os.path.join(package_folder, "train_test")
    os.makedirs(train_test_dir)

    zero_dir = os.path.join(train_test_dir, "0")
    one_dir = os.path.join(train_test_dir, "1")

    os.makedirs(zero_dir)
    os.makedirs(one_dir)

    self.create_test_file(zero_dir, "0_S_2019-09-01.png")
    self.create_test_file(zero_dir, "0_S_2019-09-03.png")
    self.create_test_file(zero_dir, "0_IBM_2019-09-03.png")
    self.create_test_file(zero_dir, "0_ROK_2019-09-03.png")
    self.create_test_file(one_dir, "1_UNP_2019-09-03.png")
    self.create_test_file(one_dir, "1_IBM_2019-09-04.png")
    self.create_test_file(one_dir, "1_S_2019-09-05.png")

    test_frac = .1
    validation_frac = .1

    # package_folder = "C:\\Users\\Chris\\workspaces\\data\\financial\\output\\stock_predictor\\selection_packages\\SelectChartZipUploadService\\process_2019-10-13_21-50-03-530.41"
    package_folder = "C:\\Users\\Chris\\workspaces\\data\\financial\\output\\stock_predictor\\selection_packages\\SelectChartZipUploadService\\process_2019-10-13_12-37-13-166.13"

    # Act
    files_to_delete, csv_path, gcs_package_path, gcs_meta_csv = project_upload_service.upload_images_and_meta_csv(package_folder=package_folder,
                                                                                                                  test_frac=test_frac,
                                                                                                                  validation_frac=validation_frac)

    self.delete_cloud_files.extend(files_to_delete)
    self.delete_cloud_files.append(gcs_meta_csv)

    # Assert
    for f in self.delete_cloud_files:
      assert (self.cloud_file_service.file_exists_in_uploads(f))

  def test_list(self):
    # Arrange

    # Act
    files = self.cloud_file_service.list_filenames("")

    # Assert
    assert (len(files) > 0)




