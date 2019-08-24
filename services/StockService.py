import os
import random
from datetime import datetime, timedelta

import pandas as pd
from pandas import DataFrame

import config
from config import logger_factory
from services import chart_service, eod_data_service
from services.Eod import Eod
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class StockService:
  DATA_FILE_TYPE_LEGACY = "StockServiceLegacy"
  DATA_FILE_TYPE_RAW = "StockServiceRaw"
  symbol_df_cache = {}

  @classmethod
  def get_cache_file_info(cls, data_file_type_name, symbol, start_date: datetime, num_days: int):
    filename = f"{data_file_type_name}-{cls.get_cache_filename(symbol, start_date, num_days)}"
    cache_file_path = os.path.join(config.constants.CACHE_DIR, filename)

    # check if hash filename exists
    does_exists = os.path.exists(cache_file_path)

    return cache_file_path, does_exists

  @classmethod
  def get_cache_filename(cls, symbol, start_date: datetime, num_days: int):
    symbol_final = symbol.lower()

    return f'{symbol_final}_{date_utils.get_standard_ymd_format(start_date)}_{num_days}.csv'

  @classmethod
  def get_random_symbols_from_df(cls, df, amount=0):
    unique_symbols = df["ticker"].unique().tolist()
    random.shuffle(unique_symbols, random.random)

    rand_symbols = unique_symbols
    if amount > 0:
      rand_symbols = unique_symbols[:amount]

    return rand_symbols

  @classmethod
  def _get_and_prep_equity_data(cls, amount_to_spend, num_days_avail, min_price, sample_file_size: SampleFileTypeSize=SampleFileTypeSize.LARGE, start_date: datetime=None, end_date: datetime=None):
    df = eod_data_service.get_shar_equity_data(sample_file_size=sample_file_size)
    df = df.sort_values(["date"])

    df_date_filtered = StockService.filter_dataframe_by_date(df, start_date, end_date)

    earliest_date_string = df_date_filtered["date"].to_list()[0]
    logger.info(f"Earliest date: {earliest_date_string}")

    end_date_string = df_date_filtered["date"].to_list()[-1]
    logger.info(f"Latest date: {end_date_string}")

    earliest_date = date_utils.parse_datestring(earliest_date_string)
    latest_date = date_utils.parse_datestring(end_date_string)

    delta = latest_date - earliest_date
    logger.info(f"Total years spanned by initial data load. {delta.days / 365}")

    df_grouped = df_date_filtered.groupby('ticker')
    # df_g_sorted = df_grouped.sort_values(by=['date'], inplace=False)

    df_g_filtered = df_grouped.filter(lambda x: EquityUtilService.filter_equity_basic_criterium(amount_to_spend, num_days_avail, min_price, x))

    return df_g_filtered

  @classmethod
  def get_sample_data(cls, output_dir: str, min_samples: int, start_date: datetime, end_date: datetime, trading_days_span: int=1000, sample_file_size: SampleFileTypeSize= SampleFileTypeSize.LARGE, persist_data=False):
    pct_gain_sought = 1.0
    min_price = 5.0
    amount_to_spend = 25000
    num_days_avail = trading_days_span

    os.makedirs(output_dir, exist_ok=True)
    df_g_filtered = cls._get_and_prep_equity_data(amount_to_spend, num_days_avail, min_price, sample_file_size)

    logger.info(f"Num with symbols after group filtering: {df_g_filtered.shape[0]}")

    df_good_list = []
    df_bad_list = []
    count = 0
    gain_list = []

    sample_info = cls.get_sample_infos(df_g_filtered, num_days_avail, min_samples, start_date=start_date, end_date=end_date)

    samples_remaining = min_samples
    while samples_remaining > 0:

      keys = sample_info.keys()
      symbol_index = random.randrange(len(keys))

      symbol = list(sample_info.keys())[symbol_index]
      assembly = sample_info[symbol]
      start_offset = assembly["offsets"].pop(0)
      total_avail_rows = assembly["total_avail_rows"]

      if len(assembly["offsets"]) == 0:
        sample_info.pop(symbol)

      logger.debug(f"total_avail_rows: {total_avail_rows}; start_offset: {start_offset}")

      df_g_symbol = cls.get_symbol_df(symbol, translate_file_path_to_hdfs=False)

      if df_g_symbol.shape[0] > 1:
        df_offset = df_g_symbol.tail(df_g_symbol.shape[0] - start_offset).head(trading_days_span)
        df_tailed = df_offset.tail(2)

        close_price = df_tailed.iloc[-2][Eod.CLOSE]
        high_price = df_tailed.iloc[-1]['high']

        logger.info(f"Ticker: -2: {df_tailed.iloc[-2]['ticker']}; -1 {df_tailed.iloc[-2]['ticker']} BuyPrice: {close_price} SellPrice: {high_price}")
        pct_gain = ((high_price - close_price) / close_price) * 100

        gain_list.append(pct_gain)

        logger.info(f"Gain: {pct_gain}")

        df_offset["assembly_index"] = count

        if pct_gain > pct_gain_sought:
          df_good_list.append(df_offset)
        else:
          df_bad_list.append(df_offset)

        count += 1

      logger.info(f"Processed {count}/{min_samples}")

      samples_remaining -= 1

    df_good_combined = pd.concat(df_good_list)
    df_bad_combined = pd.concat(df_bad_list)

    df_good_short, df_bad_short = cls.even_dataframes(df_good_combined, df_bad_combined)

    if persist_data:
      good_path = os.path.join(output_dir, f'up_{pct_gain_sought}_pct.csv')
      bad_path = os.path.join(output_dir, f'not_up_{pct_gain_sought}_pct.csv')

      df_good_short.to_csv(good_path, index=False)
      df_bad_short.to_csv(bad_path, index=False)

    return df_good_short, df_bad_short

  @classmethod
  def filter_dataframe_by_date(cls, df, start_date: datetime, end_date: datetime):
    df_date_filtered = df
    if start_date is not None:
      df_date_filtered = df[df["date"] >= date_utils.get_standard_ymd_format(start_date)]

    if end_date is not None:
      df_date_filtered = df_date_filtered[df_date_filtered["date"] <= date_utils.get_standard_ymd_format(end_date)]

    return df_date_filtered

  @classmethod
  def get_sample_infos(cls, df_g_filtered: DataFrame, num_days_avail: int, min_samples: int, translate_file_path_to_hdfs=False, start_date: datetime=None, end_date: datetime=None):
    symbols = cls.get_random_symbols(df_g_filtered)

    logger.info(f"Num symbols: {len(symbols)}")

    symb_ndx = 0
    assemblies = {}
    offset_search_count = 0
    while offset_search_count < min_samples:
      symb = symbols[symb_ndx]

      if symb not in assemblies.keys():
        logger.info(f"Getting symbol {symb}.")
        df_g_symbol = cls.get_symbol_df(symb, translate_file_path_to_hdfs)
        df_date_filtered = cls.filter_dataframe_by_date(df_g_symbol, start_date, end_date)

        total_avail_rows = df_date_filtered.shape[0]
        assemblies[symb] = {"total_avail_rows": total_avail_rows, "offsets": []}

      symb_dict = assemblies[symb]

      range_days = symb_dict["total_avail_rows"] - num_days_avail
      if range_days > len(symb_dict["offsets"]):
        start_offset = random.randrange(0, range_days)

        if start_offset not in symb_dict["offsets"]:
          symb_dict["offsets"].append(start_offset)
          logger.info(f"Got offset {start_offset} for {symb}")
          offset_search_count += 1
      else:
        logger.info(f"Reached sample saturation with symbol {symb}. No longer sampling from that symbol.")
        symbols.remove(symb)

      if symb_ndx >= len(symbols) - 1:
        symb_ndx = 0
      else:
        symb_ndx += 1

    return assemblies

  @classmethod
  def even_dataframes(cls, df1, df2):
    df1_size = df1.shape[0]
    df2_size = df2.shape[0]

    smaller_size = df1_size if df1_size < df2_size else df2_size

    return df1.iloc[:smaller_size], df2.iloc[:smaller_size]

  @classmethod
  def get_symbol_df(cls, symb, translate_file_path_to_hdfs):
    cache = cls.symbol_df_cache
    if symb in cache.keys():
      df = cache[symb]
    else:
      df = EquityUtilService.get_df_from_ticker_path(symb, translate_file_path_to_hdfs)
      cache[symb] = df

    return df

  @classmethod
  def get_random_symbols(cls, df_g_filtered, min_samples=-1):
    symbols = StockService.get_random_symbols_from_df(df_g_filtered)
    while len(symbols) < min_samples:
      symbols_extra = StockService.get_random_symbols_from_df(df_g_filtered)
      symbols.extend(symbols_extra)

    if min_samples > -1:
      symbols = symbols[:min_samples]

    return symbols

  @classmethod
  def get_eod_of_date(self, symbol, yield_date: datetime) -> dict:
    df_symbol = self.get_symbol_df(symbol, translate_file_path_to_hdfs=False)
    logger.info(f"Length of df during calc df_symbol: {df_symbol.shape[0]}")
    df_symbol = df_symbol.sort_values(by=['date'], inplace=False)

    logger.info(f"Number of days in {symbol}: {df_symbol.shape[0]}")
    logger.info(f"Start date: {df_symbol.iloc[0, :]['date']}")
    logger.info(f"End date: {df_symbol.iloc[-1, :]['date']}")
    logger.info(f"Yield Date sought: {date_utils.get_standard_ymd_format(yield_date)}")

    week_before_date = yield_date + timedelta(days=-7)
    yield_date_str = date_utils.get_standard_ymd_format(yield_date)
    week_before_date_str = date_utils.get_standard_ymd_format(week_before_date)
    df = df_symbol[(df_symbol["date"] <= yield_date_str) & (df_symbol["date"] > week_before_date_str)]
    df = df.sort_values(by=['date'], inplace=False)

    logger.info(f"Length of df during calc: {df.shape[0]}")

    bet_price = None
    yield_date = None
    open = None
    high = None
    low = None
    close = None
    if df.shape[0] >= 2:
      bet_price = df.iloc[-2,:][Eod.CLOSE]
      df_yield_day = df.iloc[-1,:]

      bet_date_str = df.iloc[-2, :]["date"]
      yield_date_str = df_yield_day["date"]
      yield_date = date_utils.parse_datestring(yield_date_str)
      logger.info(f"{symbol}: dates: {bet_date_str}; {yield_date_str}; {bet_price}; {df_yield_day['high']}")

      open = df_yield_day["open"]
      high = df_yield_day["high"]
      low = df_yield_day["low"]
      close = df_yield_day[Eod.CLOSE]

    return {
      "symbol": symbol,
      "bet_price": bet_price,
      "date": yield_date,
      "open": open,
      "high": high,
      "low": low,
      Eod.CLOSE: close,
    }

  @classmethod
  def get_stock_history_for_day(cls, df_g_filtered: pd.DataFrame, num_days_avail: int, save_dir: str, translate_file_path_to_hdfs=False, end_date: datetime = None):

    # This approximates the number of trading days needed; take num_days_avail being sampled and divide by 253 possible trading daus per year
    # This yields the number years (and +1) to look back. Multiply das in a year and we get number of days to look in the past.
    days = int((num_days_avail/253 + 1) * 365)
    start_date = end_date - timedelta(days=days)
    df_date_filtered = StockService.filter_dataframe_by_date(df_g_filtered, start_date, end_date)

    symbols = df_date_filtered["ticker"].unique().tolist()

    logger.info(f"Symbols (possible): {len(symbols)}")

    def create_image(x: pd.DataFrame, num_days_avail: int, sav_dir: str):
      logger.info(f"Getting symbol {x.iloc[0,:]['ticker']}.")
      # df_g_symbol = cls.get_symbol_df(symb, translate_file_path_to_hdfs)
      df_date_filtered = StockService.filter_dataframe_by_date(x, None, end_date)

      num_days_for_symbol = df_date_filtered.shape[0]
      if num_days_for_symbol >= num_days_avail:
        df_date_shortened = df_date_filtered.tail(num_days_avail)
        logger.info("Create image here.")
        chart_service.clean_and_save_chart(df_date_shortened, save_dir)

    df_grouped = df_date_filtered.groupby("ticker")

    df_grouped.filter(lambda x: create_image(x, num_days_avail, save_dir))

  @classmethod
  def get_random_symbols_with_date(cls, num_desired: int, desired_trading_days: int, end_date: datetime):
    df = eod_data_service.get_todays_merged_shar_data()

    symbols = df['ticker'].unique().tolist()

    random.shuffle(symbols, random.random)

    agg_symbols = []
    for s in symbols:
      df = StockService.get_symbol_df(s, translate_file_path_to_hdfs=False)
      df = df[df['date'] <= date_utils.get_standard_ymd_format(end_date)]
      df.sort_values(by=['date'], inplace=True)

      # NOTE: 2019-08-18: Bit of trickery; if there exists any records close to the end_date
      # then we know the available records abuts the end date;
      if df.shape[0] > 0:
        last_date_str = df.iloc[-1]['date']
        last_date = date_utils.parse_datestring(last_date_str)
        must_exist_date = last_date - timedelta(days=6)
        must_exist_date_str = date_utils.get_standard_ymd_format(must_exist_date)
        df_has_end_date = df[df['date'] >= must_exist_date_str]

        if df_has_end_date.shape[0] > 0 and df.shape[0] >= desired_trading_days:
          agg_symbols.append(s)
          if len(agg_symbols) >= num_desired:
            break

    return agg_symbols
