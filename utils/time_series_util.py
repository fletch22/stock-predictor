import numpy as np
from sklearn.preprocessing import MinMaxScaler

from services.StockService import StockService
from matplotlib import pyplot as plt


def sliding_windows(data: np.array, seq_length: int):
  x = []
  y = []

  for i in range(len(data) - seq_length - 1):
    _x = data[i:(i + seq_length)]
    #         print(f"_x: {_x.shape[1]}")
    _y = data[i + seq_length]
    x.append(_x)
    y.append(_y)

  return np.array(x), np.array(y)

def get_symbol_window_data(symbol: str, sliding_window_seq_len: int, feat_cols:list):
  training_set = StockService.get_symbol_df(symbol, translate_file_path_to_hdfs=False)

  # print(training_set.head())

  training_set = training_set[feat_cols]

  training_num_columns = training_set.shape[1]

  print(f"{training_set.head()}: {training_num_columns}")

  training_set_np = training_set.values

  plt.plot(training_set_np, label='LSTM')
  plt.show()

  sc = MinMaxScaler()
  training_data = sc.fit_transform(training_set_np)

  # print(f"Transformed td: {training_data.shape[1]}")

  x, y = sliding_windows(training_data, sliding_window_seq_len)

  y = np.delete(y, 0, axis=1)

  return x, y.ravel()