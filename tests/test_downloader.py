"""
pytest
TODO: download()の単体テスト
"""
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests_mock
from requests.exceptions import ConnectionError

from mexc_bulk_downloader.downloader import MexcBulkDownloader
from mexc_bulk_downloader.exceptions import (InvalidIntervalError,
                                             InvalidSymbolFormatError)

risk_reverse_response = {
    "success": True,
    "code": 0,
    "data": [
        {
            "symbol": "BTC_USDT",
            "currency": "USDT",
            "available": 25236190.842658725020122405,
            "timestamp": 1703750327370,
        },
        {
            "symbol": "ETH_USDT",
            "currency": "USDT",
            "available": 10683529.154410919248666888,
            "timestamp": 1703750327370,
        },
    ],
}


def test_make_interval():
    downloader = MexcBulkDownloader()
    assert downloader._make_interval("1m") == "Min1"
    assert downloader._make_interval("1h") == "Min60"
    with pytest.raises(InvalidIntervalError):
        downloader._make_interval("10m")


def test_validate_symbol():
    downloader = MexcBulkDownloader()
    with requests_mock.Mocker() as m:
        m.get(
            "https://contract.mexc.com/api/v1/contract/risk_reverse",
            json=risk_reverse_response,
        )
        assert downloader.validate_symbol("BTC_USDT") == "BTC_USDT"
        with pytest.raises(InvalidSymbolFormatError):
            downloader.validate_symbol("INVALID_SYMBOL")


def test_get_all_symbols_futures():
    downloader = MexcBulkDownloader()
    with requests_mock.Mocker() as m:
        m.get(
            "https://contract.mexc.com/api/v1/contract/risk_reverse",
            json=risk_reverse_response,
        )
        symbols = downloader.get_all_symbols_futures()
        assert "BTC_USDT" in symbols
        assert "ETH_USDT" in symbols


def test_get_all_symbols_futures_error(capsys):
    downloader = MexcBulkDownloader()

    with requests_mock.Mocker() as m:
        # エラーのステータスコードを返すようにモックを設定
        m.get("https://contract.mexc.com/api/v1/contract/risk_reverse", status_code=500)

        # メソッドを実行
        symbols = downloader.get_all_symbols_futures()

        # 標準出力をキャプチャして確認
        captured = capsys.readouterr()
        assert "500" in captured.out

        # シンボルリストが空であることを確認
        assert symbols is None


def test_make_url_with_valid_symbol():
    # MexcBulkDownloader インスタンスの作成
    downloader = MexcBulkDownloader()

    # 正しいシンボルの場合、期待するURLを返すかテスト
    with patch.object(downloader, "validate_symbol", return_value="BTC_USDT"):
        expected_url = "https://contract.mexc.com/api/v1/contract/kline/BTC_USDT"
        assert downloader._make_url("BTC_USDT") == expected_url


def test_make_url_with_invalid_symbol():
    # MexcBulkDownloader インスタンスの作成
    downloader = MexcBulkDownloader()

    # 不正なシンボルの場合、InvalidSymbolFormatError が発生するかテスト
    with patch.object(
        downloader,
        "validate_symbol",
        side_effect=InvalidSymbolFormatError("Invalid symbol: INVALID_SYMBOL"),
    ), pytest.raises(InvalidSymbolFormatError):
        downloader._make_url("INVALID_SYMBOL")


def test_download_all():
    downloader = MexcBulkDownloader()
    test_symbols = ["BTC_USDT", "ETH_USDT"]
    interval = "1m"

    with patch.object(
        downloader, "get_all_symbols_futures", return_value=test_symbols
    ), patch.object(downloader, "download") as mock_download, patch(
        "builtins.print"
    ) as mock_print:
        downloader.download_all(interval=interval)

        # 期待通りの回数だけdownloadメソッドが呼ばれることを確認
        assert mock_download.call_count == len(test_symbols)

        # 各シンボルに対してdownloadが呼ばれることを確認
        for symbol in test_symbols:
            mock_download.assert_any_call(symbol, None, None, interval)


def test_make_destination_dir():
    downloader = MexcBulkDownloader()
    symbol = "BTC_USDT"
    interval = "1m"

    # _destination_dirの初期設定が必要な場合、ここで設定
    downloader._destination_dir = "test_destination"

    expected_dir = "test_destination/BTC_USDT/1m"
    result = downloader._make_destination_dir(symbol, interval)

    assert result == expected_dir


def test_execute_download():
    downloader = MexcBulkDownloader()
    symbol = "BTC_USDT"
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 2)
    interval = "1m"
    expected_url = f"https://contract.mexc.com/api/v1/contract/kline/{symbol}"

    # レスポンスデータの設定
    response_data = {
        "data": [
            {"time": start_date.timestamp(), "value": 100},
            {"time": end_date.timestamp(), "value": 200},
        ]
    }

    # モック化する関数やメソッド
    with patch("requests.get") as mock_get, patch.object(
        downloader, "_make_url", return_value=expected_url
    ), patch("pandas.DataFrame.to_csv") as mock_to_csv, patch(
        "time.sleep"
    ) as mock_sleep:
        # requests.getのモックを設定
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: risk_reverse_response
        )

        # メソッド呼び出し
        downloader.execute_download(symbol, start_date, end_date, interval)

        # requests.getが呼ばれたことを確認
        mock_get.assert_called()

        # to_csvが呼ばれたことを確認
        mock_to_csv.assert_called()

        # time.sleepが呼ばれなかったことを確認
        mock_sleep.assert_not_called()


# 非200のステータスコードに対するレスポンスのテスト
def test_execute_download_non_200_response(capsys):
    downloader = MexcBulkDownloader()
    symbol = "BTC_USDT"
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 2)
    interval = "1m"
    expected_url = f"https://contract.mexc.com/api/v1/contract/kline/{symbol}"

    def mock_response(*args, **kwargs):
        if mock_get.call_count == 1:
            return MagicMock(status_code=200, json=lambda: risk_reverse_response)
        else:
            return MagicMock(status_code=404)

    with patch("requests.get", side_effect=mock_response) as mock_get, patch.object(
        downloader, "_make_url", return_value=expected_url
    ):
        downloader.execute_download(symbol, start_date, end_date, interval)

        # 標準出力をキャプチャして確認
        captured = capsys.readouterr()
        assert "404" in captured.out


def test_execute_download_connection_error(capsys):
    downloader = MexcBulkDownloader()
    symbol = "BTC_USDT"
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 2)
    interval = "1m"
    expected_url = f"https://contract.mexc.com/api/v1/contract/kline/{symbol}"

    def mock_response(*args, **kwargs):
        if mock_get.call_count == 1:
            return MagicMock(status_code=200, json=lambda: risk_reverse_response)
        else:
            raise ConnectionError

    with patch("requests.get", side_effect=mock_response) as mock_get, patch.object(
        downloader, "_make_url", return_value=expected_url
    ), patch("builtins.print") as mock_print, patch("time.sleep") as mock_sleep:
        downloader.execute_download(symbol, start_date, end_date, interval)

        # 標準出力をキャプチャして確認
        captured = capsys.readouterr()
        assert "Failed to download" in captured.out
