import torch
from seq2seq.MultivariateLstm import MultivariateLstm
import random
import numpy as np
import torch

# multivariate data preparation
from numpy import array
from numpy import hstack

# split a multivariate sequence into samples
from utils import time_series_util


def split_sequences(sequences, n_steps):
    X, y = list(), list()
    for i in range(len(sequences)):
        # find the end of this pattern
        end_ix = i + n_steps
        # check if we are beyond the dataset
        if end_ix > len(sequences):
            break
        # gather input and output parts of the pattern
        seq_x, seq_y = sequences[i:end_ix, :-1], sequences[end_ix-1, -1]
        X.append(seq_x)
        y.append(seq_y)
    return array(X), array(y)

def get_legacy_data(n_timesteps: int):
  in_seq1 = array([x for x in range(0, 100, 10)])
  in_seq2 = array([x for x in range(5, 105, 10)])
  out_seq = array([in_seq1[i] + in_seq2[i] for i in range(len(in_seq1))])
  # convert to [rows, columns] structure
  in_seq1 = in_seq1.reshape((len(in_seq1), 1))
  in_seq2 = in_seq2.reshape((len(in_seq2), 1))
  out_seq = out_seq.reshape((len(out_seq), 1))
  # horizontally stack columns
  dataset = hstack((in_seq1, in_seq2, out_seq))

  print(f"in_seq1: {in_seq1.shape}")
  print(f"in_seq2: {in_seq2.shape}")
  print(f"out_seq: {out_seq.shape}")
  print(f"dataset: {dataset.shape}")

  # convert dataset into input/output
  X, y = split_sequences(dataset, n_timesteps)

  return X, y

def run():
  n_timesteps = 3  # this is number of timesteps
  feat_cols = ['open', 'high']
  n_features = len(feat_cols)  # this is number of parallel inputs

  # define input sequence
  X, y = time_series_util.get_symbol_window_data("GOOG", n_timesteps, feat_cols)

  # X, y = get_legacy_data(n_timesteps)

  print(X.shape, y.shape)

  # raise Exception("foo")

  # create NN
  mv_net = MultivariateLstm(n_features, n_timesteps)
  criterion = torch.nn.MSELoss()  # reduction='sum' created huge loss value
  optimizer = torch.optim.Adam(mv_net.parameters(), lr=1e-1)

  train_episodes = 500
  batch_size = 16

  mv_net.train()
  for t in range(train_episodes):
    for b in range(0, len(X), batch_size):
      inpt = X[b:b + batch_size, :, :]
      target = y[b:b + batch_size]

      x_batch = torch.tensor(inpt, dtype=torch.float32)
      y_batch = torch.tensor(target, dtype=torch.float32)

      mv_net.init_hidden(x_batch.size(0))
      #    lstm_out, _ = mv_net.l_lstm(x_batch,nnet.hidden)
      #    lstm_out.contiguous().view(x_batch.size(0),-1)
      output = mv_net(x_batch)
      loss = criterion(output.view(-1), y_batch)

      loss.backward()
      optimizer.step()
      optimizer.zero_grad()
    print('step : ', t, 'loss : ', loss.item())


if __name__ == '__main__':
    run()