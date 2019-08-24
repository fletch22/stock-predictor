from unittest import TestCase

from config import logger_factory
from utils import math_utils

logger = logger_factory.create_logger(__name__)

class TestMathUtils(TestCase):

  def test_scaling(self):
    # Arrange
    # Act
    scaled = math_utils.scale_value(5, min=0, max=20)

    # Assert
    assert(scaled == 0.25)
