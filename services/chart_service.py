import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark import SparkFiles

import config
from config import logger_factory
from services.StockService import StockService

logger = logger_factory.create_logger(__name__)


def plot_and_save_from_single_file(save_dir, csv_filepath, hide_details=True):
  df = pd.read_csv(csv_filepath)

  plot_and_save(save_dir, df, hide_details)


def plot_and_save(df, save_dir, category=None, hide_details=True):
  df_sorted = df.sort_values(by=['date'], inplace=False)

  indices = StockService.get_random_assembly_indices_from_df(df)
  logger.info(f"I: {indices}")

  count = 0
  for ndx in indices:
    df_assembly = df_sorted[df_sorted["assembly_index"] == ndx]

    if category is None:
      raise Exception("Category cannot be None when hide_details is False.")
    bet_date = df_assembly["date"].tolist()[-1]
    symb = df_assembly["ticker"].tolist()[-1]

    save_data_as_chart(symb, category, df_assembly, bet_date, save_dir)

    count += 1


def save_data_as_chart(symbol: str, category: str, df_assembly: pd.DataFrame, yield_date_str: str, save_dir: str, translate_save_path_hdfs=False):
  filename_img = f"{category}_{symbol}_{yield_date_str}"

  Y = df_assembly['high'].values[:-1]
  X = np.asarray(range(0, df_assembly['high'].shape[0] - 1))
  plt.clf()

  plt.scatter(X, Y, c='black', s=1)
  # plt.plot(X, color='black',linestyle="",marker="o")

  plt.axis('off')
  filename = f"{filename_img}.png"
  save_path = os.path.join(save_dir, filename)
  if translate_save_path_hdfs:
    save_path = SparkFiles.get(save_path)
  logger.info(f"Save to {save_path}")
  plt.savefig(save_path, bbox_inches='tight', pad_inches=0, transparent=False)


def plot_and_save_from_files(file_info):
  output_dir = os.path.join(config.constants.GRAPHED_DIR)

  for f, label in file_info:
    save_dir = os.path.join(output_dir, label)
    os.makedirs(save_dir, exist_ok=True)
    plot_and_save_from_single_file(save_dir, f)
