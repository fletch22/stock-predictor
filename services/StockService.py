import os
import random
from datetime import datetime, timedelta

import pandas
import pandas as pd
from pandas import DataFrame

import config
from config import logger_factory
from services.BasicSymbolPackage import BasicSymbolPackage
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from utils import date_utils
from utils.date_utils import STANDARD_DAY_FORMAT

logger = logger_factory.create_logger(__name__)


class StockService:
  DATA_FILE_TYPE_LEGACY = "StockServiceLegacy"
  DATA_FILE_TYPE_RAW = "StockServiceRaw"
  symbol_df_cache = {}


  @classmethod
  def get_stock_from_date_strings(cls, symbol: str, start_date_str: str, num_days: int = 30):
    start_date = datetime.strptime(start_date_str, STANDARD_DAY_FORMAT)

    return cls.get_stock_legacy_format(symbol, start_date, num_days)

  @classmethod
  def get_stock_raw(cls, symbol, start_date: datetime, num_days: int):
    cache_file_path, does_exists = cls.get_cache_file_info(cls.DATA_FILE_TYPE_RAW, symbol, start_date, num_days)

    df = None
    if does_exists:
      # return to caller as DF
      df = pandas.read_csv(cache_file_path)
    else:
      # Get from SHAR_DAILY.csv
      df_full = cls.get_shar_equity_data()

      df = cls.get_stock_raw_from_dataframe(df_full, symbol, start_date, num_days)

      # Save df as file in cache folder
      df.to_csv(path_or_buf=cache_file_path)

    return df

  @classmethod
  def get_shar_equity_data(cls, sample_file_size: SampleFileTypeSize = SampleFileTypeSize.LARGE) -> DataFrame:
    file_path = config.constants.SHAR_EQUITY_PRICES
    if sample_file_size == SampleFileTypeSize.SMALL:
      file_path = config.constants.SHAR_EQUITY_PRICES_SHORT
    return pandas.read_csv(file_path)

  @classmethod
  def get_multiple_stocks(cls, basic_symbol_package: BasicSymbolPackage, df: pandas.DataFrame = None):
    all_dfs = []
    if len(basic_symbol_package.data) > 0:

      for package in basic_symbol_package.data:
        symbol = package["symbol"]
        start_date = package["start_date"]
        num_days = package["num_days"]

        cache_file_path, does_exists = cls.get_cache_file_info(cls.DATA_FILE_TYPE_RAW, symbol, start_date, num_days)

        df_symbol = None
        if does_exists:
          # return to caller as DF
          df_symbol = pandas.read_csv(cache_file_path)
        else:
          if df is None:
            df = cls.get_shar_equity_data()

          # Get from SHAR_DAILY.csv
          df_symbol = cls.get_stock_raw_from_dataframe(df, symbol, start_date, num_days)

          # Save df as file in cache folder
          df_symbol.to_csv(path_or_buf=cache_file_path)

        all_dfs.append(df_symbol)

    return all_dfs

  @classmethod
  def get_stock_raw_from_dataframe(cls, df: pandas.DataFrame, symbol, start_date: datetime, num_days: int):
    df_symbol = df[df['ticker'] == symbol.upper()]

    # Use filter on DF
    if start_date is not None:
      df_symbol = df_symbol[df_symbol['date'] > date_utils.get_standard_ymd_format(start_date)]

    df_sorted = df_symbol.sort_values(by=['date'], inplace=False)

    return df_sorted.head(num_days)

  @classmethod
  def get_cache_file_info(cls, data_file_type_name, symbol, start_date: datetime, num_days: int):
    filename = f"{data_file_type_name}-{cls.get_cache_filename(symbol, start_date, num_days)}"
    cache_file_path = os.path.join(config.constants.CACHE_DIR, filename)

    # check if hash filename exists
    does_exists = os.path.exists(cache_file_path)

    return cache_file_path, does_exists

  @classmethod
  def get_stock_legacy_format(cls, symbol, start_date: datetime, num_days: int):
    # check if hash filename exists
    cache_file_path, does_exists = cls.get_cache_file_info(cls.DATA_FILE_TYPE_LEGACY, symbol, start_date, num_days)

    df = None
    if does_exists:
      # return to caller as DF
      df = pandas.read_csv(cache_file_path)
    else:
      df_symbol = cls.get_stock_raw(symbol, start_date, num_days)

      columns = {'open': 'Open', 'date': 'Date', 'closeunadj': 'Close', 'close': 'Adj Close', 'high': 'High', 'low': 'Low', 'volume': 'Volume'}
      df_renamed = df_symbol.rename(index=str, columns=columns)

      df = df_renamed.drop(['ticker', 'dividends', 'lastupdated'], axis=1)

      cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
      df = df[cols]
      # df = df.ix[:, cols]

      # Save df as file in cache folder
      df.to_csv(path_or_buf=cache_file_path)

    return df

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
  def get_random_assembly_indices_from_df(cls, df, amount=0):
    unique_indices = df["assembly_index"].unique().tolist()
    random.shuffle(unique_indices, random.random)

    rand_symbols = unique_indices
    if amount > 0:
      rand_symbols = unique_indices[:amount]

    return rand_symbols

  @classmethod
  def _get_and_prep_equity_data(cls, amount_to_spend, num_days_avail, min_price, sample_file_size: SampleFileTypeSize=SampleFileTypeSize.LARGE):
    df = cls.get_shar_equity_data(sample_file_size=sample_file_size)
    df = df.sort_values(["date"])
    earliest_date_string = df["date"].to_list()[0]
    logger.info(f"Earliest date: {earliest_date_string}")

    end_date_string = df["date"].to_list()[-1]
    logger.info(f"Latest date: {end_date_string}")

    earliest_date = date_utils.parse_datestring(earliest_date_string)
    latest_date = date_utils.parse_datestring(end_date_string)

    delta = latest_date - earliest_date
    logger.info(f"Total years spanned by initial data load. {delta.days / 365}")

    df_grouped = df.groupby('ticker')

    def filter_grouped(x):
      first_row = x.iloc[0]
      start_day_volume = first_row['volume']
      last_row = x.iloc[-1]
      yield_day_volume = last_row['volume']
      close_price = last_row['close']
      shares_bought = amount_to_spend / close_price

      # Shares bought should be greater than .01% of the yield day's volume.
      # Shares bought should be greater than .005% of the start span day's volume. So
      return len(x) > num_days_avail \
             and close_price >= min_price \
             and shares_bought > (.0001 * yield_day_volume) \
             and start_day_volume > (yield_day_volume/2)

    df_g_filtered = df_grouped.filter(lambda x: filter_grouped(x))

    return df_g_filtered

  @classmethod
  def get_sample_data(cls, output_dir, min_samples, trading_days_span: int=1000, sample_file_size: SampleFileTypeSize= SampleFileTypeSize.LARGE, persist_data=False):
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

    sample_info = cls.get_sample_infos(df_g_filtered, num_days_avail, min_samples)

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

        close_price = df_tailed.iloc[-2]['close']
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

      df_good_short.to_csv(good_path)
      df_bad_short.to_csv(bad_path)

    return df_good_short, df_bad_short


  @classmethod
  def get_sample_infos(cls, df_g_filtered: DataFrame, num_days_avail: int, min_samples: int, translate_file_path_to_hdfs=False):
    symbols = cls.get_random_symbols(df_g_filtered)

    logger.info(f"Num symbols: {len(symbols)}")

    symb_ndx = 0
    assemblies = {}
    offset_search_count = 0
    while offset_search_count < min_samples:
      symb = symbols[symb_ndx]

      if symb not in assemblies.keys():
        df_g_symbol = cls.get_symbol_df(symb, translate_file_path_to_hdfs)
        total_avail_rows = df_g_symbol.shape[0]
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
      df_g_symbol = cache[symb]
    else:
      df_g_symbol = EquityUtilService.get_df_from_ticker_path(symb, translate_file_path_to_hdfs)
      cache[symb] = df_g_symbol

    return df_g_symbol

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
  def get_eod_of_date(self, symbol, date: datetime) -> dict:
    df_symbol = self.get_symbol_df(symbol, translate_file_path_to_hdfs=False)
    df_symbol = df_symbol.sort_values(by=['date'])

    logger.info(f"Number of days in {symbol}: {df_symbol.shape[0]}")
    logger.info(f"Start date: {df_symbol.iloc[0, :]['date']}")
    logger.info(f"End date: {df_symbol.iloc[-1, :]['date']}")
    logger.info(f"Date sought: {date_utils.get_standard_ymd_format(date)}")

    week_before_date = date + timedelta(days=-7)
    bet_date_str = date_utils.get_standard_ymd_format(date)
    week_before_date_str = date_utils.get_standard_ymd_format(week_before_date)
    df = df_symbol[(df_symbol["date"] <= bet_date_str) & (df_symbol["date"] > week_before_date_str)]
    df = df.sort_values(by=['date'])

    bet_price = df.iloc[-2,:]["close"]
    df_yield_day = df.iloc[-1,:]

    return {
      "symbol": symbol,
      "bet_price": bet_price,
      "date": date_utils.parse_datestring(df_yield_day["date"]),
      "open": df_yield_day["open"],
      "high": df_yield_day["high"],
      "low": df_yield_day["low"],
      "close": df_yield_day["close"],
    }
