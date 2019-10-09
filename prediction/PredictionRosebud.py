from datetime import datetime
from typing import Sequence

from charts.ChartMode import ChartMode
from charts.ChartType import ChartType
from utils.ClassValidator import ClassValidator


class PredictionRosebud(ClassValidator):
  score_threshold: float = None
  sought_gain_frac: float = None
  pct_gain_sought: float = None
  max_files: int = None
  yield_date: datetime = None
  num_days_to_sample = 1000
  min_price = 5.0
  chart_type: ChartType = None
  chart_mode: ChartMode = None
  volatility_min: float = None
  add_realtime_price_if_missing: bool = None
  min_volume = None

  def is_valid(self) -> (bool, Sequence[str]):
    result = True
    msgs = []

    result, msg = self.must_be_positive_and_not_none("score_threshold", result, msgs)
    result, msg = self.must_be_not_none("sought_gain_frac", result, msgs)
    result, msg = self.must_be_not_none("pct_gain_sought", result, msgs)
    result, msg = self.must_be_positive_and_not_none("max_files", result, msgs)
    result, msg = self.must_be_date("yield_date", result, msgs)
    result, msg = self.must_be_not_none("num_days_to_sample", result, msgs)
    result, msg = self.must_be_not_none("min_price", result, msgs)
    result, msg = self.must_be_not_none("min_volume", result, msgs)
    result, msg = self.must_be_not_none("chart_type", result, msgs)
    result, msg = self.must_be_not_none("chart_mode", result, msgs)
    result, msg = self.must_be_not_none("volatility_min", result, msgs)
    result, msg = self.must_be_not_none("chart_type", result, msgs)
    result, msg = self.must_be_not_none("volatility_min", result, msgs)
    result, msg = self.must_be_not_none("add_realtime_price_if_missing", result, msgs)


    return result, msg
