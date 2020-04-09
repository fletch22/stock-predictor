from datetime import datetime

import torch

import numpy as np
from pandas import DataFrame

from config import logger_factory
from seq2seq.UberModel import UberModel
from services.EquityUtilService import EquityUtilService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

def create_dataset(df: DataFrame, look_back=1, forecast_horizon=1, batch_size=1):
  batch_x, batch_y, batch_z = [], [], []
  for i in range(0, len(df) - look_back - forecast_horizon - batch_size + 1, batch_size):
    for n in range(batch_size):
      x = df.values[i + n:(i + n + look_back), :]
      offset = x[0, 0]
      y = df['high'].values[i + n + look_back:i + n + look_back + forecast_horizon]
      batch_x.append(np.array(x).reshape(look_back, -1))
      batch_y.append(np.array(y))
      batch_z.append(np.array(offset))
    batch_x = np.array(batch_x)
    batch_y = np.array(batch_y)
    batch_z = np.array(batch_z)
    batch_x[:, :, 0] -= batch_z.reshape(-1, 1)
    batch_y -= batch_z.reshape(-1, 1)
    yield batch_x, batch_y, batch_z
    batch_x, batch_y, batch_z = [], [], []

def get_dataframe(symbol: str):
  df = EquityUtilService.get_df_from_ticker_path(symbol=symbol, translate_to_hdfs_path=False)

  logger.info(f"Columns {df.columns}")

  epoch = datetime.utcfromtimestamp(0)

  df = df.drop(columns=['ticker'])

  df = df[df['date'] < '2019-01-01']

  def convert_to_seconds(x):
    seconds = (date_utils.parse_std_datestring(x) - epoch).total_seconds() * 1000
    return seconds

  df['date'] = df['date'].apply(lambda x: convert_to_seconds(x))

  df = df.drop(columns=['lastupdated'])

  logger.info(f"Header: {df.head(100)}")

  return df


def train():
  batch_size = 1
  forecast_horizon = 1
  look_back = 28
  model = UberModel(dict(features=8, forecast_horizon=1))
  optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
  n_epochs = 5


  model.train()
  train_true_y = []
  train_pred_y = []

  df = get_dataframe("GOOG")

  for epoch in range(n_epochs):
      ep_loss = []
      for i, batch in enumerate(create_dataset(df, look_back=look_back, forecast_horizon=1, batch_size=batch_size)):
          print("[{}{}] Epoch {}: loss={:0.4f}".format("-"*(20*i//(len(df)//batch_size)), " "*(20-(20*i//(len(df)//batch_size))),epoch, np.mean(ep_loss)), end="\r")
          try:
              batch = [torch.Tensor(x) for x in batch]
          except:
              break

          out = model.forward(batch[0].float(), batch_size)
          loss = model.loss(out, batch[1].float())
          if epoch == n_epochs - 1:
              train_true_y.append((batch[1] + batch[2]).detach().numpy().reshape(-1))
              train_pred_y.append((out + batch[2]).detach().numpy().reshape(-1))
          optimizer.zero_grad()
          loss.backward()
          optimizer.step()
          ep_loss.append(loss.item())
      print()


if __name__ == '__main__':
    train()