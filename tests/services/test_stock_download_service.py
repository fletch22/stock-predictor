from services import stock_download_service
from utils import date_utils


def test_get_actions():
    # Arrange
    # Action
    stock_download_service.download_actions()