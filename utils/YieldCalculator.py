from typing import Any, Union

from config import logger_factory
from utils import date_utils
from utils.CalculationPackage import CalculationPackage

logger = logger_factory.create_logger(__name__)

class YieldCalculator:
  count_gainers: Union[int, Any]

  def __init__(self, score_threshold: float, initial_investment_amount: int, sought_gain_frac: float):
    self.score_threshold = score_threshold
    self.initial_investment_amount = initial_investment_amount
    self.sought_gain_frac = sought_gain_frac
    self.aggregate_gains = []
    self.count_gainers: int = 0
    self.count_predicted: int = 0
    self.earned_amount = self.initial_investment_amount
    self.gains_map = {}

  def calc_frac_gain(self, calculationPackage: CalculationPackage):
    symbol = calculationPackage.symbol
    bet_price = calculationPackage.bet_price
    low = calculationPackage.low
    high = calculationPackage.high
    close = calculationPackage.close
    score = calculationPackage.score
    cat_predicted = calculationPackage.category_predicted
    cat_actual = calculationPackage.category_actual
    date_str = calculationPackage.date_str
    max_price_drop_frac = calculationPackage.max_price_drop_frac

    panic_sell_price = 0
    if max_price_drop_frac is not None:
      panic_sell_price = bet_price + (bet_price * max_price_drop_frac)

    if cat_predicted == "1" and score >= self.score_threshold:
      self.count_predicted += 1
      if cat_actual == cat_predicted:
        sought_gain_price = bet_price + (bet_price * self.sought_gain_frac)

        if low < panic_sell_price:
          frac_return = max_price_drop_frac
        else:
          if high >= sought_gain_price:
            frac_return = self.sought_gain_frac
          else:
            frac_return = (close - bet_price) / bet_price

        self.count_gainers += 1
      else:
        if low < panic_sell_price:
          frac_return = max_price_drop_frac
        else:
          frac_return = (close - bet_price) / bet_price

      self.add_to_gains(date_str, frac_return)

      pct_gain = round(frac_return * 100, 2)
      logger.info(f"Date: {date_str}; Symbol: {symbol}; {pct_gain}%")

  def add_to_gains(self, date_str: str, frac_return: float):
    frac_ret_for_date = []
    if date_str in self.gains_map.keys():
      frac_ret_for_date = self.gains_map.get(date_str)

    frac_ret_for_date.append(frac_return)

    self.gains_map[date_str] = frac_ret_for_date