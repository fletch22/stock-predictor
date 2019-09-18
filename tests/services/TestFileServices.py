import os
from unittest import TestCase

import config
from services import file_services


class TestFileServices(TestCase):

  def test_file_created_today(self):
    # Arrange
    file_path = config.constants.SHAR_EQUITY_PRICES
    # Act
    created_today = file_services.file_modified_today(file_path)

    # Assert
    assert(not created_today)

  def test_foo(self):
    dirs = file_services.get_folders_in_dir("C:\\Users\\Chris\\workspaces\\data\\financial\\output\\stock_predictor\\selection_packages\\SelectChartZipUploadService\\tmp_folder_spot_09-08")

    dir_names = [os.path.basename(d) for d in dirs if os.path.basename(d).startswith("process")]

    str = [f"\"{d}\"" for d in dir_names]

    print(",\n".join(str))

