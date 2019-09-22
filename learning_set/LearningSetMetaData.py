from datetime import datetime
from typing import Sequence, List

from charts.ChartType import ChartType
from services import pickle_service


class LearningSetMetaData():
  min_price: float = None
  amount_to_spend: float = None
  trading_days_span: int = None
  min_samples: int = None
  pct_gain_sought: float = None
  start_date: datetime = None
  end_date: datetime = None
  pct_test_holdout: float = None
  chart_type: ChartType = None
  volatility_min: float = None

  def is_valid(self) -> (bool, Sequence[str]):
    result = True
    msgs = []

    result, msg = self.must_be_positive_and_not_none("min_price", result, msgs)
    result, msg = self.must_be_positive_and_not_none("amount_to_spend", result, msgs)
    result, msg = self.must_be_positive_nonzero_and_not_none("min_samples", result, msgs)
    result, msg = self.must_be_positive_nonzero_and_not_none("pct_test_holdout", result, msgs)
    result, msg = self.must_be_not_none("chart_type", result, msgs)
    result, msg = self.must_be_positive_nonzero_and_not_none("volatility_min", result, msgs)

    return result, msg

  def must_be_positive_and_not_none(self, attribute_name: str, is_valid_so_far: bool, messages: List[str]) -> (bool, List[str]):
    is_valid = is_valid_so_far
    attr_value = getattr(self, attribute_name)
    if attr_value is None:
      messages.append(f"{attribute_name} is None.")
      is_valid = False
    elif attr_value < 0:
      messages.append(f"{attribute_name} is less than 0.")
      is_valid = False

    return is_valid, messages

  def must_be_positive_nonzero_and_not_none(self, attribute_name: str, is_valid_so_far: bool, messages: List[str]) -> (bool, List[str]):
    is_valid = is_valid_so_far
    attr_value = getattr(self, attribute_name)
    if attr_value is None:
      messages.append(f"{attribute_name} is None.")
      is_valid = False
    elif attr_value <= 0:
      messages.append(f"{attribute_name} is less than 0.")
      is_valid = False

    return is_valid, messages

  def must_be_not_none(self, attribute_name: str, is_valid_so_far: bool, messages: List[str]) -> (bool, List[str]):
    is_valid = is_valid_so_far
    attr_value = getattr(self, attribute_name)
    if attr_value is None:
      messages.append(f"{attribute_name} is None.")
      is_valid = False

    return is_valid, messages

  def persist(self, file_path: str):
    pickle_service.save(self, file_path)

  @classmethod
  def load(cls, file_path: str):
    pickle_service.load(file_path)

  @classmethod
  def pickle_filename(cls):
    return "learning_set_metadata.pkl"
