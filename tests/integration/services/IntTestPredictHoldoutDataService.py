from datetime import datetime
from unittest import TestCase

from config import logger_factory
from services.PredictHoldoutDataService import PredictHoldoutDataService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestPredictHoldoutDataService(TestCase):

  def test_predict_day(self):
    # Arrange
    short_model_id = "ICN200769567050768296"  # voleq_rec Sept %

    yield_date_strs = [
      '2019-08-23'
    ]

    # Act
    for yield_date_str in yield_date_strs:
      yield_date = date_utils.parse_datestring(yield_date_str)
      image_dir = PredictHoldoutDataService.get_predictions_on_date(yield_date, short_model_id=short_model_id, start_sample_date=None)

      # Assert
      assert(image_dir is not None)
      logger.info(f"Images written to '{image_dir}'")

