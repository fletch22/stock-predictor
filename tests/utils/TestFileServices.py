import os
import random
import shutil
from unittest import TestCase
from utils import date_utils

import config
from categorical.BinaryCategoryType import BinaryCategoryType
from config import logger_factory
from services import file_services

logger = logger_factory.create_logger(__name__)

class TestFileServices(TestCase):
  delete_dirs = []

  def create_random_output_dir(self):
    randy = str(random.randrange(10 ** 8))
    rand_dir = os.path.join(config.constants.OUTPUT_DIR, randy)
    self.delete_dirs.append(rand_dir)

    os.makedirs(rand_dir)

    return rand_dir

  def tearDown(self):
    for d in self.delete_dirs:
      shutil.rmtree(d)

  def test_create_unique_folder(self):
    # Arrange
    randy = random.randrange(10**8)
    parent_dir = os.path.join(config.constants.OUTPUT_DIR, str(randy))
    self.delete_dirs.append(parent_dir)

    # Act
    unit_test_dir_1 = file_services.create_unique_folder(parent_dir, "unit_test")
    unit_test_dir_2 = file_services.create_unique_folder(parent_dir, "unit_test")

    logger.info(f"created: {unit_test_dir_1}")
    logger.info(f"created: {unit_test_dir_2}")

    # Assert
    assert(unit_test_dir_1 != unit_test_dir_2)
    assert(os.path.exists(unit_test_dir_1))
    assert(os.path.exists(unit_test_dir_2))

  def test_zip_dir(self):
    # Arrange
    dir_to_zip = self.create_random_output_dir()
    output_dir = self.create_random_output_dir()

    rand_file_path = os.path.join(dir_to_zip, 'test.txt')
    f = open(rand_file_path,"w+")
    f.write("This is a test.")
    f.close()

    output_path = os.path.join(output_dir, "test.zip")

    # Act
    output_path = file_services.zip_dir(dir_to_zip, output_path)

    # Assert
    assert(os.path.exists(output_path))

  def test_thing(self):
    # Arrange
    package_folder = "process_2019-08-07_08-27-02-182.53"
    holdout_dir = os.path.join(config.constants.TEST_DATA_FILES, package_folder, "test_holdout")

    # Act
    files = file_services.truncate_older(holdout_dir)

    # Assert
    files.sort(key=lambda f: f["date"])
    youngest_file_date = files[0]['date']

    assert(date_utils.get_standard_ymd_format(youngest_file_date) == '2019-07-12')


