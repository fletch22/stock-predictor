from unittest import TestCase

from config import logger_factory
from services.PredictHoldoutDataService import PredictHoldoutDataService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class TestPredictHoldoutDataService(TestCase):

  def test_predict_day(self):
    # Arrange
    # 2019-08-15; ICN2383257928398147071; Accuracy: 0.1702127659574468; total: 47; mean frac: -0.018434153490591357; total: 4139.6201230658935
    yield_date = date_utils.parse_datestring('2019-07-17')
    short_model_id = "ICN2383257928398147071"

    # Act
    image_dir = PredictHoldoutDataService.get_predictions_on_date(yield_date, short_model_id=short_model_id)

    # Assert
    assert(image_dir is not None)
    logger.info(f"Images written to '{image_dir}'")
