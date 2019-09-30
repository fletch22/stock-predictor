from datetime import datetime
from typing import List


class ClassValidator:

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

  def must_be_date(self, attribute_name: str, is_valid_so_far: bool, messages: List[str]) -> (bool, List[str]):
    is_valid = is_valid_so_far
    attr_value = getattr(self, attribute_name)
    if attr_value is None:
      messages.append(f"{attribute_name} is None.")
      is_valid = False
    elif type(attr_value) != type(datetime.now()):
      messages.append(f"{attribute_name} is not a valid datetime type. It's a '{type(attr_value)}' type.")
      is_valid = False

    return is_valid, messages
