import json
import os
from unittest import TestCase

from array import array
from sklearn.preprocessing import MinMaxScaler

import numpy as np
import config
from config import logger_factory
from min_max.ListMinMaxer import ListMinMaxer

logger = logger_factory.create_logger(__name__)

class TestMinMaxCarrier(TestCase):

  def test_min_save(self):
    # Arrange
    package_path = os.path.join(config.constants.TEST_PACKAGE_FOLDER)

    list_min_maxer = ListMinMaxer(package_path)

    file_path = list_min_maxer._get_file_path()
    if os.path.exists(file_path):
      os.remove(file_path)

    assert(not os.path.exists(file_path))

    the_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    fundies = {'pe': the_list}

    # Act
    list_min_maxer.persist(fundies)

    # Assert
    list_min_maxer.load()

    result = list_min_maxer.scale(5.5, 'pe')

    assert(result == 0.49999999999999994)

