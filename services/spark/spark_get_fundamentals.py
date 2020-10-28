from datetime import datetime
from typing import Sequence, Dict

import findspark
from pyspark import SparkContext

from config import logger_factory
from services.EquityUtilService import EquityUtilService
from services.StockService import StockService
from services.equities import equity_fundamentals_service
from utils import date_utils

logger = logger_factory.create_logger(__name__)


# Expects Example: {'MODLF': {'total_avail_rows': 1968, 'offsets': [841]}, 'BSX': {'total_avail_rows': 2651, 'offsets': [1502]}, 'IRT': {'total_avail_rows': 1491, 'offsets': [92]}, 'STNG': {'total_avail_rows': 2339, 'offsets': [128]}, 'AMC': {'total_avail_rows': 1402, 'offsets': [29]}, 'GWW': {'total_avail_rows': 2651, 'offsets': [313]}, 'GLMD': {'total_avail_rows': 1345, 'offsets': [22]}, 'NGHCP': {'total_avail_rows': 1276, 'offsets': [39]}, 'MINI': {'total_avail_rows': 2651, 'offsets': [1085]}, 'CPS': {'total_avail_rows': 2299, 'offsets': [661]}}
# Returns Example: [{'symbol': 'MODLF', 'trading_days_span': 1000, 'start_date': '<date>', 'end_date': '<date>', 'desired_fundmamentals': ['pe', 'ev'], 'total_avail_rows': 1968, 'offsets': [841]}, etc]
def convert_to_spark_array(sample_infos: Dict, trading_days_span: int, start_date: datetime, end_date: datetime, desired_fundamentals: Sequence[str]):
    symbol_arr = []
    for symb_key in sample_infos.keys():
        sym_info = sample_infos[symb_key]
        sym_info['symbol'] = symb_key
        sym_info['trading_days_span'] = trading_days_span
        sym_info['start_date'] = start_date
        sym_info['end_date'] = end_date
        sym_info['desired_fundamentals'] = desired_fundamentals
        symbol_arr.append(sym_info)

    return symbol_arr


def do_spark(spark_arr: Sequence, num_slices=None):
    logger.info(f"Spark arr: {spark_arr}")

    findspark.init()
    sc = SparkContext.getOrCreate()
    sc.stop()

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")
    logger.info(sc._jsc.sc().uiWebUrl().get())

    rdd = sc.parallelize(spark_arr, num_slices)
    logger.info("About to invoke rdd map method ...")
    results = rdd.map(spark_process_sample_info).collect()

    logger.info(f"Got {len(results)} results.")

    sc.stop()

    return results


def spark_process_sample_info(symbol_info: Dict) -> Dict:
    translate_to_hdfs = True
    logger.info(f"Symbol_info: {symbol_info}")

    symbol = symbol_info["symbol"]
    logger.info(f"Getting fundamentals for {symbol}...")

    offsets = symbol_info["offsets"]
    trading_days_span = symbol_info['trading_days_span']
    start_date = symbol_info["start_date"]
    end_date = symbol_info["end_date"]
    desired_fundamentals = symbol_info['desired_fundamentals']

    df = EquityUtilService.get_df_from_ticker_path(symbol, translate_to_hdfs)
    df_sorted = df.sort_values(by=['date'], inplace=False)
    df_date_filtered = StockService.filter_dataframe_by_date(df_sorted, start_date, end_date)

    logger.info(f"Size df: {df_date_filtered.shape[0]}")

    fundamentals = {
        "symbol": symbol,
        "offset_info": {}
    }
    offset_info = fundamentals['offset_info']

    for start_offset in offsets:
        logger.info(f'StartOffset: {start_offset}')

        df_offset = df_date_filtered.tail(df_date_filtered.shape[0] - start_offset).head(trading_days_span)

        logger.info(f"df_offset length: {df_offset.shape[0]}")

        bet_date_str = df_offset.iloc[-2]['date']

        logger.info(f"bet date: {bet_date_str}")

        bet_date = date_utils.parse_std_datestring(bet_date_str)

        fundies = equity_fundamentals_service.get_multiple_values(symbol, bet_date, desired_fundamentals, df=None, translate_to_hdfs=translate_to_hdfs)

        logger.debug(f"fundies: {fundies}")

        offset_info[start_offset] = fundies

    return fundamentals
