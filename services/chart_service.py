import os
import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark import SparkFiles

from config import logger_factory

logger = logger_factory.create_logger(__name__)

def plot_and_save_for_learning(df, save_dir, category):
  df_sorted = df.sort_values(by=['date'], inplace=False)

  indices = get_random_assembly_indices_from_df(df)
  logger.info(f"I: {indices}")

  count = 0
  for ndx in indices:
    df_assembly = df_sorted[df_sorted["assembly_index"] == ndx]

    bet_date = df_assembly["date"].tolist()[-1]
    symb = df_assembly["ticker"].tolist()[-1]

    save_data_as_chart(symb, category, df_assembly, bet_date, save_dir)

    count += 1

def get_random_assembly_indices_from_df(df, amount=0):
  unique_indices = df["assembly_index"].unique().tolist()
  random.shuffle(unique_indices, random.random)

  rand_symbols = unique_indices
  if amount > 0:
    rand_symbols = unique_indices[:amount]

  return rand_symbols

def clean_and_save_chart(df: pd.DataFrame, save_dir: str):
  symb = df.iloc[0,:]["ticker"]
  yield_date_str = df.iloc[-1,:]['date']

  save_data_as_chart_no_category(symb, df, yield_date_str, save_dir)

def save_data_as_chart(symbol: str, category: str, df: pd.DataFrame, yield_date_str: str, save_dir: str, translate_save_path_hdfs=False):
  filename_prefix = f"{category}_{symbol}_{yield_date_str}"

  save_chart_to_filename(df, save_dir, filename_prefix, translate_save_path_hdfs)

def save_data_as_chart_no_category(symbol: str, df: pd.DataFrame, yield_date_str: str, save_dir: str, translate_save_path_hdfs=False):
  filename_prefix = f"{symbol}_{yield_date_str}"

  save_chart_to_filename(df, save_dir, filename_prefix, translate_save_path_hdfs)

def save_chart_to_filename(df_assembly: pd.DataFrame, save_dir: str, filename_prefix: str, translate_save_path_hdfs=False):
  Y = df_assembly['high'].values[:-1]
  X = np.asarray(range(0, df_assembly['high'].shape[0] - 1))

  plt.clf()

  plt.scatter(X, Y, c='black', s=1)
  # plt.plot(X, color='black',linestyle="",marker="o")

  plt.axis('off')
  filename = f"{filename_prefix}.png"
  save_path = os.path.join(save_dir, filename)
  if translate_save_path_hdfs:
    save_path = SparkFiles.get(save_path)
  logger.info(f"Save to {save_path}")
  plt.savefig(save_path, bbox_inches='tight', pad_inches=0, transparent=False)
