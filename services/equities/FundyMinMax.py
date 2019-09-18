# from datetime import datetime
# from typing import Dict
#
# import pandas as pd
#
# from services import eod_data_service
# from services.StockService import StockService
# from services.equities.FullDataEquityFundamentalsService import FullDataEquityFundamentalsService
# from utils import date_utils
#
#
# class FundyMinMax:
#   min_max_map = {}
#
#   def __init__(self, sample_info: Dict[str, Dict], start_date: datetime, end_date: datetime, trading_days_span: int):
#
#     fdefs = FullDataEquityFundamentalsService()
#
#     for symbol in sample_info.keys():
#       symbol_info = sample_info[symbol]
#
#       df = StockService.get_symbol_df(symbol, translate_file_path_to_hdfs=False)
#       df_sorted = df.sort_values(by=['date'], inplace=False)
#       df_date_filtered = StockService.filter_dataframe_by_date(df_sorted, start_date, end_date)
#
#       offset_info_array = []
#       self.min_max_map[symbol] = offset_info_array
#       for offset in symbol_info['offsets']:
#         df_offset = df_date_filtered.tail(df_date_filtered.shape[0] - offset).head(trading_days_span)
#
#         offset_start_date = date_utils.parse_datestring(df_offset.iloc[0]['date'])
#         offset_end_date = date_utils.parse_datestring(df_offset.iloc[-1]['date'])
#
#         fundamentals = fdefs.get_fundamentals_at_point_in_time(symbol, offset_start_date, offset_end_date)
#
#         offset_info = {
#           "offset": offset,
#           "start_date": offset_start_date,
#           "end_date": offset_end_date,
#           "fundamentals": fundamentals
#         }
#         offset_info_array.append(offset_info)
