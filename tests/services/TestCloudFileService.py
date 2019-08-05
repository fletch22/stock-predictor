import os
import shutil
from unittest import TestCase

import config
from services import file_services
from services.CloudFileService import CloudFileService


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