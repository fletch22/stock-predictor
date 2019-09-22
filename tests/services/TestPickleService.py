import os
from unittest import TestCase

import config
from services import pickle_service


class TestPickleService(TestCase):

  def test_save(self):
    # Arrange
    thing = {'foo': 'bar'}
    file_path = os.path.join(config.constants.CACHE_DIR, "foo.bar")

    os.remove(file_path)
    assert (not os.path.exists(file_path))

    # Act
    pickle_service.save(thing, file_path)
    assert (os.path.exists(file_path))

  def test_load(self):
    # Arrange
    thing = {'foo': 'bar'}
    file_path = os.path.join(config.constants.CACHE_DIR, "foo.bar")

    os.remove(file_path)
    assert (not os.path.exists(file_path))

    pickle_service.save(thing, file_path)

    # Act
    thing_actual = pickle_service.load(file_path)
    assert(thing_actual['foo'] == 'bar')

