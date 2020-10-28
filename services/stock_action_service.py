from config import constants
import pandas as pd

def get_splits():
    df = pd.read_csv(constants.SHARADAR_ACTIONS_FILEPATH)
    return df[df["action"] == "split"]

stock_splits = get_splits()