from unittest import TestCase

import numpy as np
import pandas as pd

from config import logger_factory
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class TestDateUtils(TestCase):

  def test_numpy(self):
    arr = [1, 2, 3, 4, 5]

    df = pd.DataFrame(arr, columns=["A"])

    X = np.linspace(0, 1, num=5) ** 2
    logger.info(F"type: {type(X)}")
    # np_arr = np.array(arr)

    y = df["A"]
    ahead = y.shift(-1)
    new_y = 100.0 * (ahead / y - 1)

    logger.info(f"ahead: {ahead}")
    logger.info(f"new_y: {new_y}")

    test = 1
    assert (test == 1)

  def test_convert_wtd_to_utc(self):
    # Arrange
    # Act
    dt_utc = date_utils.convert_wtd_nyc_date_to_utc("2019-06-20 16:00:01")

    # Assert
    logger.info(f"{dt_utc}")
    assert("2019-06-20 20:00:01+00:00" == dt_utc.__str__())
