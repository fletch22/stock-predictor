import math


def scale_value(value, min, max):
  return (value - min) / (max - min)


def is_neither_nan_nor_none(value):
  return (value is not None) and (math.isnan(value) == False)