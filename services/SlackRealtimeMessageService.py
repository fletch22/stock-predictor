from datetime import datetime

from slack import RTMClient

import config
from config.logger_factory import logger_factory
from services.BetHistoryService import BetHistoryService
from utils import type_utils, date_utils

logger = logger_factory.create_logger(__name__)


class CommandTypes:
  Bought = "Bought"


@RTMClient.run_on(event="message")
def bot_respond(**payload):
  data = payload['data']
  web_client = payload['web_client']

  if 'text' in data and data['text'].startswith(CommandTypes.Bought):
    command = data['text']
    channel_id = data['channel']
    thread_ts = data['ts']

    threw_exception = False
    try:
      num_shares, symbol, price = get_bet(command)
    except Exception as e:
      web_client.chat_postMessage(
        channel=channel_id,
        text=f"Did not seem to understand '{CommandTypes.Bought}' command. Try something like 'Bought 111 ibm@123.45'",
        thread_ts=thread_ts
      )
      threw_exception = True

    if not threw_exception:
      bet_history_service = BetHistoryService()
      now = datetime.now()
      bet_history_service.add_bet(num_shares=num_shares, symbol=symbol, purchase_price=price, date=now)
      now_str = date_utils.get_standard_ymd_format(now)

      web_client.chat_postMessage(
        channel=channel_id,
        text=f"Purchase Noted: num_shares: {num_shares}; symbol: '{symbol}'; purchase_price: {price}, date: {now_str}",
        thread_ts=thread_ts
      )


def get_bet(command: str) -> (float, str, float):
  tokens = command.split()
  if len(tokens) != 3:
    raise Exception("Could not parse 'command'.")

  num_shares = tokens[1]
  if not type_utils.is_number(num_shares):
    raise Exception("Could not parse 'num_shares'.")
  num_shares = float(num_shares)

  symb_price = tokens[2].split("@")
  if len(symb_price) != 2:
    raise Exception("Symbol and price could not parse.")

  symbol = symb_price[0]
  price = symb_price[1]
  if not type_utils.is_number(price):
    raise Exception("Could not parse 'price'.")
  price = float(price)

  return num_shares, symbol, price


if __name__ == '__main__':
  credentials = config.constants.SLACK_CREDENTIALS
  rtm_client = RTMClient(token=credentials['bot_token_id'])
  rtm_client.start()
