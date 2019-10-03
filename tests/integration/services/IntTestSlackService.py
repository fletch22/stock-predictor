from unittest import TestCase

from services.SlackService import SlackService


class IntTestSlackService(TestCase):

  def test_slack_connection(self):
      # Arrange
      slack_service = SlackService()

      message = "TeST TEST TEST"

      # Act
      response = slack_service.send_direct_message_to_chris(message)

      # Assert
      assert response["ok"]