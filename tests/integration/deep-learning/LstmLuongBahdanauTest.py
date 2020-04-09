import os
from unittest import TestCase
import pandas as pd

import config
from config.logger_factory import create_logger, logger_factory
from deep_learning.lstm.LstmLuongBahdanau import LstmLuongBahdanau

logger = logger_factory.create_logger(__name__)

class LstmLuongBahdanauTest(TestCase):

  def test_score_acc(self):
    # Arrange
    future_days = 50
    name_suffix = '_lstm_luong.csv'
    act_path = os.path.join(config.constants.PROJECT_DIR, "deep-learning", f"actual{name_suffix}")
    df_act = pd.read_csv(act_path)

    hist_path = os.path.join(config.constants.PROJECT_DIR, "deep-learning", f"hist{name_suffix}")
    df_hist = pd.read_csv(hist_path)

    pred_path = os.path.join(config.constants.PROJECT_DIR, "deep-learning", f"pred{name_suffix}")
    df_pred = pd.read_csv(pred_path)

    # 1183.7
    df_hist_fut = df_hist.iloc[-1:, :]
    df_act_fut = df_act.iloc[-future_days:-future_days + 1, :]
    df_pred_fut = df_pred.iloc[-future_days:-future_days + 1:,:]
    df_pred_hist_fut = df_pred.iloc[-(future_days + 1):-(future_days + 0):,:]

    logger.info(f"{df_pred_hist_fut.head()}")

    logger.info(f"Size: {df_pred_hist_fut.shape[0]}")

    pred_hist_high_mean = df_pred_hist_fut['High'].mean()
    hist_high_mean = df_hist_fut['High'].mean()
    act_high_mean = df_act_fut['High'].mean()
    pred_high_mean = df_pred_fut['High'].mean()
    logger.info(f"hist: {hist_high_mean}; pred_hist: {pred_hist_high_mean}; Act: {act_high_mean}; Pred: {pred_high_mean}")

    # Act
    # Assert

  def test_file(self):
    # Arrange
    symbols = ["F"]
    # symbols = ['AAPL', 'ABBV', 'ABEV', 'ACB', 'AKS', 'AMD', 'APHA', 'APRN', 'AR', 'AUY', 'AVP', 'BABA', 'BAC', 'BMY', 'C', 'CGIX', 'CHK', 'CLDR', 'CLF', 'CMCSA', 'CPE', 'CRZO', 'CSCO', 'CTL', 'CY', 'CZR', 'DAL', 'DB', 'DNR', 'ECA', 'ET', 'F', 'FB', 'FCEL', 'FCX', 'FHN', 'GBTC', 'GE', 'GGB', 'GM', 'GNMX', 'GOLD', 'HAL', 'HL', 'HSGX', 'IMRN', 'INFY', 'INTC', 'ITUB', 'JD', 'JNJ', 'JPM', 'KEY', 'KMI', 'MGTI', 'MPW', 'MRO', 'MS', 'MSFT', 'MSRT', 'MU', 'NBR', 'NIO', 'NLY', 'NOK', 'ORCL', 'OXY', 'PBCT', 'PBR', 'PDD', 'PFE', 'QCOM', 'RF', 'RIG', 'ROKU', 'RRC', 'S', 'SAN', 'SCHW', 'SIRI', 'SLB', 'SLS', 'SNAP', 'SWN', 'SYMC', 'T', 'TEVA', 'TRNX', 'TRQ', 'TWTR', 'UBNK', 'VALE', 'VZ', 'WDC', 'WFC', 'WMB', 'WPX', 'X', 'XOM', 'ZNGA']

    lstm = LstmLuongBahdanau()

    # Act
    all_processed_files = lstm.process_many(symbols)

    # Arrange
    assert(len(all_processed_files) == len(symbols) * 3)