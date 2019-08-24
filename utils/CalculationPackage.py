from datetime import datetime

import math


class CalculationPackage:
  symbol: str = None
  bet_price: float = None
  high: float = None
  low: float = None
  close: float = None
  date_str: str = None
  category_predicted: str = None
  category_actual: str = None
  score: float = None
  max_price_drop_frac: float = None

  def validate(self):
    return self.symbol is not None \
           and self.bet_price is not None and not math.isnan(self.bet_price) \
           and self.high is not None and not math.isnan(self.high) \
           and self.low is not None and not math.isnan(self.low) \
           and self.close is not None and not math.isnan(self.close) \
           and self.date_str is not None \
           and self.category_predicted is not None \
           and self.category_actual is not None \
           and self.score is not None and not math.isnan(self.score)