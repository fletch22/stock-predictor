from utils import date_utils

class BasicSymbolPackage:
  data = list()

  def add_symbol(self, symbol: str, start_datestring: str, num_days: int):
    date = {
      "symbol": symbol,
      "start_date": date_utils.parse_datestring(start_datestring),
      "num_days": num_days
    }

    self.data.append(date)
