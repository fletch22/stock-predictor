from stopwatch import Stopwatch

from config import logger_factory
from services import eod_data_service
from services.spark import spark_split_eod_data
import pandas as pd
logger = logger_factory.create_logger(__name__)

def process(df: pd.DataFrame=None):
  stopwatch = Stopwatch()
  stopwatch.start()
  split_info = split_shar_equity_to_ticker_files(df)
  stopwatch.stop()

  logger.info(f"Split Elapsed Seconds: {stopwatch}.")

  return split_info

def split_shar_equity_to_ticker_files(df: pd.DataFrame=None):
  if df is None:
    df = eod_data_service.get_todays_merged_shar_data()

  stock_infos = spark_split_eod_data.use_spark_to_split(df)
  return stock_infos

if __name__ == "__main__":
  logger.info("Run from command line...")
  process()