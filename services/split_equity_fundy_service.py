import pandas as pd
from stopwatch import Stopwatch

from config import logger_factory
from services.equities.FullDataEquityFundamentalsService import FullDataEquityFundamentalsService
from services.spark import spark_split_fundy_data

logger = logger_factory.create_logger(__name__)

def process(df: pd.DataFrame=None):
  stopwatch = Stopwatch()
  stopwatch.start()
  split_info = split_equity_fundy_to_ticker_files(df)
  stopwatch.stop()

  logger.info(f"Split Elapsed Seconds: {stopwatch}.")

  return split_info

def split_equity_fundy_to_ticker_files(df: pd.DataFrame=None):
  if df is None:
    efs = FullDataEquityFundamentalsService()
    df = efs.df

  stock_infos = spark_split_fundy_data.use_spark_to_split(df)
  return stock_infos


if __name__ == "__main__":
  logger.info("Run from command line...")
  process()
