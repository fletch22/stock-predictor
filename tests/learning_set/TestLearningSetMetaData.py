import os
from unittest import TestCase

import config
from config.logger_factory import logger_factory
from learning_set.LearningSetMetaData import LearningSetMetaData

logger = logger_factory.create_logger(__name__)

class TestLearningSetMetaData(TestCase):

  def test_is_valid(self):
    # Arrange
    learning_set_metadata = LearningSetMetaData()

    # Act
    is_valid, msgs = learning_set_metadata.is_valid()

    # Assert
    assert(not is_valid)
    assert(len(msgs) > 0)
    message_1 = msgs[0]
    assert(message_1 == "min_price is None.")

  def test_persist(self):
    # Arrange
    learning_set_metadata = LearningSetMetaData()
    file_path = os.path.join(config.constants.CACHE_DIR, "test_learning_set_metata.pkl")

    if (os.path.exists(file_path)):
      os.remove(file_path)

    assert (not os.path.exists(file_path))

    # Act
    learning_set_metadata.persist(file_path)

    # Assert
    assert(os.path.exists(file_path))
    os.remove(file_path)
    assert (not os.path.exists(file_path))

  def test_load(self):
    # Arrange
    lms_expected = LearningSetMetaData()
    lms_expected.min_price = 123.34
    file_path = os.path.join(config.constants.CACHE_DIR, "test_learning_set_metata.pkl")

    if (os.path.exists(file_path)):
      os.remove(file_path)

    assert (not os.path.exists(file_path))
    lms_expected.persist(file_path)

    # Act
    lms_actual = LearningSetMetaData.load(file_path)

    # Assert
    assert(lms_actual.min_price == lms_expected.min_price)
    os.remove(file_path)

