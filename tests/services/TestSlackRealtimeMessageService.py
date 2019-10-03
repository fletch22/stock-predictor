from unittest import TestCase

from services import SlackRealtimeMessageService


class TestSlackRealtimeMessageService(TestCase):

  def test_get_bet(self):
    # Arrange
    command = "Bought 111 ibm@5.12"

    # Act
    num_shares, symbol, price = SlackRealtimeMessageService.get_bet(command)

    # Assert
    assert(num_shares == 111)
    assert(symbol == 'ibm')
    assert(price == 5.12)