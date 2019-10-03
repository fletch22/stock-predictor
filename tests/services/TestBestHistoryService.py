import os
from datetime import datetime

import config
from services.BetHistoryService import BetHistoryService
import pandas as pd


class TestBestHistoryService:

  def test_add_bet(self):
    # Arrange
    bet_history_service = BetHistoryService()

    bet_hist_path = os.path.join(config.constants.CACHE_DIR, 'tmp_bet_history.csv')
    if os.path.exists(bet_hist_path):
      os.remove(bet_hist_path)

    bet_history_service.csv_path = bet_hist_path
    bet_history_service.df = None
    num_shares = 777

    # Act
    bet_history_service.add_bet(num_shares=num_shares, symbol='IBM', purchase_price='123.45', date=datetime.now())

    # Assert
    df = pd.read_csv(bet_hist_path)
    assert(df.shape[0] == 1)
