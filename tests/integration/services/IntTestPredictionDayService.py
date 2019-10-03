from unittest import TestCase

from services import prediction_day_service


class IntTestPredictionDayService(TestCase):

  def test_current_rosebud_predictor(self):
    # Arrange
    # Assert
    symbols = prediction_day_service.current_rosebud_predictor()

    # Act
    assert(len(symbols) > 0)