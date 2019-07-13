import os

PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))
WORKSPACE_DIR = os.path.dirname(PROJECT_DIR)
DATA_DIR = os.path.join(WORKSPACE_DIR, "data")
SHAR_DATA_DIR = os.path.join(DATA_DIR, "sharadar")
SHAR_DAILY = os.path.join(SHAR_DATA_DIR, "SHAR_DAILY.csv")

CACHE_DIR = os.path.join(SHAR_DATA_DIR, "cache")

if not os.path.isdir(CACHE_DIR):
  os.makedirs(CACHE_DIR, exist_ok=True)