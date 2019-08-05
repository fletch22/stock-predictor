from datetime import datetime

STANDARD_DAY_FORMAT = '%Y-%m-%d'
DATE_WITH_SECONDS_FORMAT = '%Y-%m-%d_%H-%M-%S'

def parse_datestring(datestring):
  return datetime.strptime(datestring, STANDARD_DAY_FORMAT)

def get_standard_ymd_format(date: datetime):
  return date.strftime(STANDARD_DAY_FORMAT)

def format_file_system_friendly_date(date: datetime):
  millis = round(date.microsecond / 1000, 2)
  return f"{date.strftime(DATE_WITH_SECONDS_FORMAT)}-{millis}"