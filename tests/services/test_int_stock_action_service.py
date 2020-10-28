from services import stock_action_service


def test_get_stock_splits():
    df = stock_action_service.get_splits()

    assert(df.shape[0] > 0)