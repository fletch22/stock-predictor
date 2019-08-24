import matplotlib.pyplot as plt

def chart(symbol, df_close, states_buy, states_sell, total_gains, total_investment):
  fig = plt.figure(figsize=(15, 5))
  plt.plot(df_close, color='r', lw=2.)
  plt.plot(df_close, '^', markersize=10, color='m', label='buying signal', markevery=states_buy)
  plt.plot(df_close, 'v', markersize=10, color='k', label='selling signal', markevery=states_sell)
  plt.title(f"{symbol} total gains {round(total_gains, 2)}, total investment {round(total_investment, 2)}")
  plt.legend()
  plt.show()