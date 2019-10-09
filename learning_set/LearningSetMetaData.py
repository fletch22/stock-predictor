from datetime import datetime
from typing import Sequence, List

from charts.ChartType import ChartType
from services import pickle_service
from utils.ClassValidator import ClassValidator


class LearningSetMetaData(ClassValidator):
  min_price: float = None
  trading_days_span: int = None
  min_samples: int = None
  pct_gain_sought: float = None
  start_date: datetime = None
  end_date: datetime = None
  pct_test_holdout: float = None
  chart_type: ChartType = None
  volatility_min: float = None
  min_volume: int = None

  def is_valid(self) -> (bool, Sequence[str]):
    result = True
    msgs = []

    result, msg = self.must_be_positive_and_not_none("min_price", result, msgs)
    result, msg = self.must_be_positive_and_not_none("min_volume", result, msgs)
    result, msg = self.must_be_positive_nonzero_and_not_none("min_samples", result, msgs)
    result, msg = self.must_be_positive_nonzero_and_not_none("pct_test_holdout", result, msgs)
    result, msg = self.must_be_not_none("chart_type", result, msgs)
    result, msg = self.must_be_positive_nonzero_and_not_none("volatility_min", result, msgs)

    return result, msg

  def persist(self, file_path: str):
    pickle_service.save(self, file_path)

  @classmethod
  def load(cls, file_path: str):
    pickle_service.load(file_path)

  @classmethod
  def pickle_filename(cls):
    return "learning_set_metadata.pkl"
