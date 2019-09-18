import os
import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image
from pyspark import SparkFiles

from config import logger_factory

logger = logger_factory.create_logger(__name__)


# def plot_and_save_for_learning(df, save_dir, category):
#   df_sorted = df.sort_values(by=['date'], inplace=False)
#
#   indices = get_random_assembly_indices_from_df(df)
#   logger.info(f"I: {indices}")
#
#   count = 0
#   for ndx in indices:
#     df_assembly = df_sorted[df_sorted["assembly_index"] == ndx]
#
#     bet_date = df_assembly["date"].tolist()[-1]
#     symb = df_assembly["ticker"].tolist()[-1]
#
#     save_data_as_chart(symb, category, df_assembly, bet_date, save_dir)
#
#     count += 1


def get_random_assembly_indices_from_df(df, amount=0):
  unique_indices = df["assembly_index"].unique().tolist()
  random.shuffle(unique_indices, random.random)

  rand_symbols = unique_indices
  if amount > 0:
    rand_symbols = unique_indices[:amount]

  return rand_symbols


def save_data_as_chart(symbol: str, category: str, df: pd.DataFrame, yield_date_str: str, save_dir: str, translate_save_path_hdfs=False):
  filename_prefix = f"{category}_{symbol}_{yield_date_str}"

  save_vanilla_chart_to_filename(df, save_dir, filename_prefix, translate_save_path_hdfs)


def save_data_as_chart_no_category(symbol: str, df: pd.DataFrame, yield_date_str: str, save_dir: str, translate_save_path_hdfs=False):
  filename_prefix = f"{symbol}_{yield_date_str}"

  save_vanilla_chart_to_filename(df, save_dir, filename_prefix, translate_save_path_hdfs)


def save_vanilla_chart_to_filename(df_assembly: pd.DataFrame, save_dir: str, filename_prefix: str, translate_save_path_hdfs=False):
  Y = df_assembly['high'].values[:-1]
  X = np.asarray(range(0, df_assembly['high'].shape[0] - 1))

  plt.clf()

  plt.scatter(X, Y, c='black', s=1)

  plt.axis('off')
  filename = f"{filename_prefix}.png"
  save_path = os.path.join(save_dir, filename)
  if translate_save_path_hdfs:
    save_path = SparkFiles.get(save_path)
  logger.info(f"Save to {save_path}")
  plt.savefig(save_path, bbox_inches='tight', pad_inches=0, transparent=False)


def save_decorated_chart_to_filename(df: pd.DataFrame, fundy: dict, category: str, symbol: str, yield_date_str: str,
                                     save_dir: str, translate_save_path_hdfs=False):
  plt = render_decorated_chart(df, fundy=fundy)

  filename = f"{category}_{symbol}_{yield_date_str}.png"
  save_path = os.path.join(save_dir, filename)
  if translate_save_path_hdfs:
    save_path = SparkFiles.get(save_path)
  logger.info(f"Save to {save_path}")

  plt.savefig(save_path, bbox_inches='tight', pad_inches=0, transparent=False)


def render_decorated_chart(df_assembly: pd.DataFrame, fundy: dict):
  Y = df_assembly['high'].values[:-1]
  X = np.asarray(range(0, df_assembly['high'].shape[0] - 1))

  plt.clf()

  plt.figure(0)
  ax1 = plt.subplot2grid((6, 6), (0, 0), colspan=6, rowspan=5)
  ax2 = plt.subplot2grid((6, 6), (5, 0), colspan=6, rowspan=1)

  ax1.scatter(X, Y, c='black', s=1)
  ax1.set_axis_off()

  pe = get_fund_for_chart(fundy, 'pe')
  ev = get_fund_for_chart(fundy, 'ev')
  eps = get_fund_for_chart(fundy, 'eps')

  metrics = [1.1, eps, pe, ev]
  y_pos = np.arange(len(metrics))
  performance = metrics

  # NOTE: Make the first unused bar invisible. We need it to spread the all the bars to the whole distance
  width = [0.01, 1.0, 1.0, 1.0]

  ax2.barh(y_pos, performance, height=width, color=['black', 'red', 'green', 'cyan'], align='center')
  ax2.set_axis_off()

  return plt


# Expects example: { 'pe': 18.0; 'ev': 1.2345564342 }
def get_fund_for_chart(fund_info, fund_key):
  value = 0
  if fund_key in fund_info.keys():
    value = fund_info[fund_key]
    if value is None:
      value = .1  # A zero value forces the other bars to expand overmuch.
    else:
      value += .1

  return value


def add_image_to_plot(plt):
  im = Image.open('/home/jofer/logo.png')
  height = im.size[1]

  # We need a float array between 0-1, rather than
  # a uint8 array between 0-255
  im = np.array(im).astype(np.float) / 255

  plt.plot(np.arange(10), 4 * np.arange(10))

  fig = plt.figure()

  # With newer (1.0) versions of matplotlib, you can
  # use the "zorder" kwarg to make the image overlay
  # the plot, rather than hide behind it... (e.g. zorder=10)
  fig.figimage(im, 0, fig.bbox.ymax - height)
