import os
from datetime import datetime, timedelta

import pandas as pd

import config
from config import logger_factory
from services import file_services, split_eod_data_to_files_service, stock_download_service
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils

logger = logger_factory.create_logger(__name__)


def get_todays_merged_shar_data():
  merged_path = config.constants.SHAR_EQUITY_PRICES_MERGED
  if os.path.exists(merged_path) and file_services.file_modified_today(merged_path):
    df_merged = pd.read_csv(merged_path).sort_values(by=['date'])
  else:
    df = get_shar_equity_data()
    df_merged = merge_shar_equity_price_data(df)

    df_merged.to_csv(config.constants.SHAR_EQUITY_PRICES_MERGED, index=False)
    split_eod_data_to_files_service.process(df_merged)

  return df_merged


def get_shar_equity_data(sample_file_size: SampleFileTypeSize = SampleFileTypeSize.LARGE) -> pd.DataFrame:
  file_path = config.constants.SHAR_EQUITY_PRICES
  if sample_file_size == SampleFileTypeSize.SMALL:
    file_path = config.constants.SHAR_EQUITY_PRICES_SHORT
  df = pd.read_csv(file_path)
  return df.sort_values(by=['date'], inplace=False)


def merge_shar_equity_price_data(df_base: pd.DataFrame):
  missing = find_and_download_missing_days(df_base, datetime.now())

  supp_files = []
  for m in missing:
    supp_files.append(m['supplemental_path'])

  # files = file_services.walk(config.constants.SHAR_EQUITY_PRICES_DIR)
  # supp_files = [f for f in files if os.path.basename(f).startswith("supp_")]
  logger.info(f"Found {len(supp_files)} supplemental files.")

  sort_crit = ['ticker', 'date']
  df_merged = df_base.sort_values(by=sort_crit, inplace=False)
  for supp in supp_files:
    logger.info(f"Adding {os.path.basename(supp)}.")
    df_supp = pd.read_csv(supp)
    df_supp.sort_values(by=sort_crit, inplace=True)

    df_merged = pd.concat([df_merged, df_supp], ignore_index=False, sort=False)

    logger.info(f"df_merged size: {df_merged.shape[0]}")

  df_unduped = df_merged.drop_duplicates(sort_crit)

  logger.info(f"df_unduped size: {df_unduped.shape[0]}")

  df_unduped.sort_values(by=['date'], inplace=True)

  return df_unduped


def find_and_download_missing_days(df: pd.DataFrame, until_date: datetime):
  df.sort_values(by=['date'], inplace=True)

  end_date_str = df.iloc[-1, :]["date"]
  end_date = date_utils.parse_datestring(end_date_str)

  next_date = end_date + timedelta(days=1)

  results = []
  while next_date <= until_date:
    next_plus_one = next_date + timedelta(days=1)
    filename = get_supplemental_filename(next_date, next_plus_one)
    print(f"Getting next filename: {filename}")
    file_path = os.path.join(config.constants.SHAR_EQUITY_PRICES_DIR, filename)

    if not os.path.exists(file_path):
      stock_download_service.download_sep(next_date, next_plus_one, file_path)

    results.append({'next_date': next_date, 'supplemental_path': file_path})

    next_date = next_date + timedelta(days=1)

  return results


def get_supplemental_filename(start_date: datetime, end_date: datetime):
  return f"supp_{date_utils.get_standard_ymd_format(start_date)}_{date_utils.get_standard_ymd_format(end_date)}.csv"
