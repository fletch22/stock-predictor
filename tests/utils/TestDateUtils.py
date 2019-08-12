import os
from unittest import TestCase

import numpy as np
import pandas as pd

import config
from config import logger_factory
from services import chart_service

logging = logger_factory.create_logger(__name__)


class TestDateUtils(TestCase):

  def test_numpy(self):
    arr = [1, 2, 3, 4, 5]

    df = pd.DataFrame(arr, columns=["A"])

    X = np.linspace(0, 1, num=5) ** 2
    logging.info(F"type: {type(X)}")
    # np_arr = np.array(arr)

    y = df["A"]
    ahead = y.shift(-1)
    new_y = 100.0 * (ahead / y - 1)

    logging.info(f"ahead: {ahead}")
    logging.info(f"new_y: {new_y}")

    test = 1
    assert (test == 1)
