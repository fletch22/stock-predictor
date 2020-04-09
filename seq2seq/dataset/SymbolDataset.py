import os
from datetime import datetime

import torch
from torch.utils.data import Dataset
import pandas as pd
import numpy as np
import config
from config.logger_factory import logger_factory
from services.EquityUtilService import EquityUtilService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class SymbolDataset(Dataset):
  """
      Support class for the loading and batching of sequences of samples

      Args:
          dataset (Tensor): Tensor containing all the samples
          sequence_length (int): length of the analyzed sequence by the LSTM
          transforms (object torchvision.transform): Pytorch's transforms used to process the data
  """

  def __init__(self, symbol:str, sequence_length=1, transforms=None, numrows=None, mns_file_name=None):
    df = EquityUtilService.get_df_from_ticker_path(symbol=symbol, translate_to_hdfs_path=False)

    epoch = datetime.utcfromtimestamp(0)

    df = df.drop(columns=['ticker'])

    def convert_to_seconds(x):
      seconds = (date_utils.parse_std_datestring(x) - epoch).total_seconds() * 1000
      return seconds

    df['date'] = df['date'].apply(lambda x: convert_to_seconds(x))

    df = df.drop(columns=['lastupdated'])

    logger.info(f"Columns {df.columns}")

    df = df.drop(columns=['date', 'volume', 'dividends', 'closeunadj'])

    logger.info(f"Header: {df.head(100)}")

    s = torch.Size([1, 2, 3])
    logger.info(f"s header: {s.head()}")
    torch.tensor(s)

    raise Exception("foo")

    self.seq_length = sequence_length
    self.tensor_data = torch.from_numpy(df.values.astype(np.float32))
    self.transforms = transforms

  def __len__(self):
    return len(self.tensor_data)

  ##  Override single items' getter
  def __getitem__(self, idx):
    if idx + self.seq_length > self.__len__():
      if self.transforms is not None:
        item = torch.zeros(self.seq_length, self.tensor_data[0].__len__())
        item[:self.__len__() - idx] = self.transforms(self.tensor_data[idx:])
        return item, item
      else:
        item = []
        item[:self.__len__() - idx] = self.tensor_data[idx:]
        return item, item
    else:
      if self.transforms is not None:
        return self.transforms(self.tensor_data[idx:idx + self.seq_length]), self.transforms(self.tensor_data[idx:idx + self.seq_length])
      else:
        return self.tensor_data[idx:idx + self.seq_length], self.tensor_data[idx:idx + self.seq_length]
