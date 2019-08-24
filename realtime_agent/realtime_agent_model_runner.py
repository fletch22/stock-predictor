import os
import pickle
from datetime import datetime

import numpy as np
from sklearn.preprocessing import MinMaxScaler

import config
from realtime_agent.Agent import Agent
from realtime_agent.Model import Model
from services.EquityToLegacyDataConversionService import EquityToLegacyDataConversionService


def run(symbol: str, trading_days_desired: int, latest_date: datetime=None):
  skip = 1
  # file_path = os.path.join(config.constants.PROJECT_DIR, "realtime_agent", "model_new.pkl")
  file_path = os.path.join(config.constants.DATA_DIR, "models", "stock-predictor", "agents", "realtime_agents", "model_realtime_agent_evolution_1.pkl")

  assert (os.path.exists(file_path))

  with open(file_path, "rb") as f:
    model: Model = pickle.load(f)

  if datetime is None:
    latest_date = datetime.now()
  df = EquityToLegacyDataConversionService.get_symbol(symbol, num_records=trading_days_desired, latest_date=latest_date)
  real_trend = df['Close'].tolist()
  parameters = [df['Close'].tolist(), df['Volume'].tolist()]
  minmax = MinMaxScaler(feature_range=(100, 200)).fit(np.array(parameters).T)
  scaled_parameters = minmax.transform(np.array(parameters).T).T.tolist()
  initial_money = np.max(parameters[0]) * 2

  agent = Agent(model=model,
                timeseries=scaled_parameters,
                skip=skip,
                initial_money=initial_money,
                real_trend=real_trend,
                minmax=minmax)

  agent.change_data(timeseries=scaled_parameters,
                    skip=skip,
                    initial_money=initial_money,
                    real_trend=real_trend,
                    minmax=minmax)

  states_buy, states_sell, total_gains, invest = agent.buy()

  return df['Close'], states_buy, states_sell, total_gains, invest
