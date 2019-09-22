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
