from unittest import TestCase

from utils import random_utils


class TestRandomUtils(TestCase):

  def test_select_random_from_list(self):
    # Arrange
    the_list = ["Abc", "Cde", "Efg"]

    # Act
    sel_items = random_utils.select_random_from_list(the_list, 2)

    # Assert
    assert(len(sel_items) == 2)
    assert(len(the_list) == 3)