from datetime import datetime
from unittest import TestCase

import pandas as pd

from config import logger_factory
from services import eod_data_service
from services.EquityUtilService import EquityUtilService
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class TestEquityUtilService(TestCase):

  def test_is_today_missing(self):
    # Arrange
    df = eod_data_service.get_todays_merged_shar_data()
    df = df.sort_values(["date"])

    today_str = date_utils.get_standard_ymd_format(datetime.now())

    # NOTE: Remove today (if present) for test.
    df_not_today = df[df['date'] < today_str]

    # Act
    is_missing = EquityUtilService.is_missing_today(df_not_today)

    # Assert
    assert(is_missing)

  def test_is_today_not_missing(self):
    # Arrange
    df = eod_data_service.get_todays_merged_shar_data()
    df = df.sort_values(["date"])

    today_str = date_utils.get_standard_ymd_format(datetime.now())

    df_one_day = df[df['date'] == '2019-08-30']
    df_one_day['date'] = today_str

    df_merged = pd.concat([df, df_one_day])

    # Act
    is_missing = EquityUtilService.is_missing_today(df_merged)

    # Assert
    assert(not is_missing)
