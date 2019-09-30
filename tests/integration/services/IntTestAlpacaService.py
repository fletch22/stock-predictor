from unittest import TestCase

from config.logger_factory import logger_factory
from services.AlpacaService import AlpacaService

logger = logger_factory.create_logger(__name__)


class IntTestAlpacaService(TestCase):

  def test_get_account(self):
      # Arrange
      alpaca_service = AlpacaService()

      # Act
      account = alpaca_service.get_account()

      # Assert
      assert(account is not None)


