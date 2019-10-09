from datetime import datetime

from config import logger_factory
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class EodCalculator:

  @classmethod
  def make_nova_calculation(cls, aggregate_gain, score_threshold:float, bet_price, category_actual: str, category_predicted: str, open: float, high: float, low: float, close: float, count: int, initial_investment_amount: float,
                            max_drop: float, score: float, sought_gain_frac: float, symbol: str, yield_date: datetime):
      earned_amount = initial_investment_amount

      if bet_price is None:
        return count, earned_amount, aggregate_gain

      logger.info(f"{symbol} Yield Date: {date_utils.get_standard_ymd_format(yield_date)} Score: {score}; score_threshold: {score_threshold}")
      max_drop_price = bet_price - (max_drop * bet_price)
      logger.info(f"\tbet_price: {bet_price}; open: {open}; high: {high}; low: {low}; close: {close}; max_drop_price: {max_drop_price}; ")
      if score >= score_threshold:
        if open > bet_price:
          frac_return = ((open - bet_price) / bet_price)
          aggregate_gain.append(frac_return)
          count += 1
        elif category_actual == category_predicted:
          sought_gain_price = bet_price + (bet_price * sought_gain_frac)

          # if low < max_drop_price:
          #   frac_return = -1 * max_drop
          #   aggregate_gain.append(frac_return)
          # el
          if high > sought_gain_price:
            frac_return = sought_gain_frac
            aggregate_gain.append(frac_return)
          else:
            frac_return = (close - bet_price) / bet_price
            aggregate_gain.append(frac_return)

          count += 1
        else:
          # if low < max_drop_price:
          #   aggregate_gain.append(-1 * max_drop)
          # else:
          frac_return = (close - bet_price) / bet_price
          aggregate_gain.append(frac_return)

        returnAmount = earned_amount * frac_return
        former_amount = earned_amount
        earned_amount = earned_amount + (earned_amount * frac_return)

        if earned_amount < 0:
          earned_amount = 0

        logger.info(f"\t{former_amount} + {returnAmount} = {earned_amount}")

      return count, earned_amount, aggregate_gain

  @classmethod
  def make_early_bird_calculation(cls, aggregate_gain, score_threshold: float, bet_price, category_actual: str, category_predicted: str, open: float, high: float, low: float, close: float, count: int,
                            initial_investment_amount: float,
                            max_drop: float, score: float, sought_gain_frac: float, symbol: str, yield_date: datetime):
    earned_amount = initial_investment_amount

    if bet_price is None:
      return count, earned_amount, aggregate_gain

    logger.info(f"{symbol} Yield Date: {date_utils.get_standard_ymd_format(yield_date)} Score: {score}; score_threshold: {score_threshold}")
    max_drop_price = bet_price - (max_drop * bet_price)
    logger.info(f"\tbet_price: {bet_price}; open: {open}; high: {high}; low: {low}; close: {close}; max_drop_price: {max_drop_price}; ")
    if score >= score_threshold and open < bet_price:
      bet_price = open
      sought_gain_price = bet_price + (bet_price * sought_gain_frac)

      if high >= sought_gain_price:
        frac_return = sought_gain_frac
        aggregate_gain.append(frac_return)
        count += 1
      else:
        frac_return = (close - bet_price) / bet_price
        aggregate_gain.append(frac_return)

      returnAmount = earned_amount * frac_return
      former_amount = earned_amount
      earned_amount = earned_amount + (earned_amount * frac_return)

      if earned_amount < 0:
        earned_amount = 0

      logger.info(f"\t{former_amount} + {returnAmount} = {earned_amount}")

    return count, earned_amount, aggregate_gain
