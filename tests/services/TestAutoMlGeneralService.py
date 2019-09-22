from unittest import TestCase

from config import logger_factory
from services.AutoMlGeneralService import AutoMlGeneralService

logger = logger_factory.create_logger(__name__)


class TestAutoMlPredictionService(TestCase):

  def test_sort_by_date_in_basename(self):
    # Arrange
    files = [
      'foo/0_ABC_2019-02-01.png',
      'foo/0_ABC_2019-01-01.png',
      'foo/1_ABC_2019-04-01.png',
      'foo/1_ABC_2019-04-02.png',
      'foo/0_ABC_2019-03-01.png'
    ]

    # Act
    files_sorted = AutoMlGeneralService.sort_by_date_in_basename(files)

    # Assert
    assert (files_sorted[0] == 'foo/0_ABC_2019-01-01.png')
    assert (files_sorted[1] == 'foo/0_ABC_2019-02-01.png')
    assert (files_sorted[2] == 'foo/0_ABC_2019-03-01.png')
    assert (files_sorted[3] == 'foo/1_ABC_2019-04-01.png')
    assert (files_sorted[-1] == 'foo/1_ABC_2019-04-02.png')
