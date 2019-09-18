import json
import os
from typing import Sequence, Dict

from joblib import dump, load
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd

from config import logger_factory

logger = logger_factory.create_logger(__name__)

NOT_SET = -1
SAVE_FILENAME = "min_max_carrier.json"

class ListMinMaxer:
  min = None
  max = None

  def __init__(self, package_path):
    self.package_path = package_path
    self.min_max_scalers = None

  def _get_file_path(self):
    meta_folder = os.path.join(self.package_path, "learning_set_meta")
    os.makedirs(meta_folder, exist_ok=True)

    return os.path.join(meta_folder, SAVE_FILENAME)

  def persist_from_df(self, df: pd.DataFrame, desired_columns: Sequence[str]):
    fundies = {}
    for col in desired_columns:
      fundies[col] = df[col].values.tolist()

    self.persist(fundies)

  def persist(self, fundies: Dict[str, Sequence]) -> str:
    file_path = self._get_file_path()

    self.min_max_scalers = {}
    for key in fundies.keys():
      values = fundies[key]
      self.min_max_scalers[key] = self._get_min_max_scaler(values)

    dump(self.min_max_scalers, file_path)

    return file_path

  def _get_min_max_scaler(self, values: Sequence) -> MinMaxScaler:
    scaler = MinMaxScaler()

    arr_new = np.array(values).reshape(-1, 1)
    arr_new = arr_new.astype('float64')
    scaler.fit(arr_new)

    return scaler

  def load(self) -> Dict[str, MinMaxScaler]:
    file_path = self._get_file_path()

    self.min_max_scalers = load(file_path)

    return self.min_max_scalers

  def scale(self, val: float, fundamental: str):
    if self.min_max_scalers is None:
      raise Exception("Scaler has not been set. Perhaps you have not loaded it yet?")

    arr_value = np.array(val).reshape(1, -1)
    arr_value = arr_value.astype('float64')

    return self.min_max_scalers[fundamental].transform(arr_value)[0][0]





