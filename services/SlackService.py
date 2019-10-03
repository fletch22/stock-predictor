import os

import slack

import config

USER_ID_CHRIS = "U4J5M51CK"

class SlackService:

  def __init__(self):
    credentials = config.constants.SLACK_CREDENTIALS
    self.client = slack.WebClient(token=credentials['bot_token_id'])

  def send_direct_message_to_chris(self, message):
    userChannel = self.client.conversations_open(users=[USER_ID_CHRIS])

    return self.client.chat_postMessage(
      text=message,
      channel=userChannel['channel']['id']
    )
