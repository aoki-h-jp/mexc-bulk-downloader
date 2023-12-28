"""
mexc_bulk_downloader
"""

# import standard libraries
import os
import time
import warnings
from datetime import datetime, timedelta

import pandas as pd
# import third-party libraries
import requests
from rich import print
from rich.progress import track

from mexc_bulk_downloader.exceptions import (InvalidIntervalError,
                                             InvalidSymbolFormatError)

warnings.filterwarnings("ignore")


class MexcBulkDownloader:
    _MEXC_BASE_URL = "https://contract.mexc.com"
    _INTERVALS = {
        "1m": "Min1",
        "5m": "Min5",
        "15m": "Min15",
        "30m": "Min30",
        "1h": "Min60",
        "4h": "Hour4",
        "8h": "Hour8",
        "1d": "Day1",
        "1w": "Week1",
        "1M": "Month1",
    }

    _INTERVALS_MINUTES = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "4h": 240,
        "8h": 480,
        "1d": 1440,
        "1w": 10080,
        "1M": 43200,
    }

    def __init__(
        self,
        destination_dir=".",
    ):
        """
        :param destination_dir: Directory to save the downloaded data.
        """
        self._destination_dir = destination_dir + "/mexc_data"

    def _make_interval(self, interval: str = "1m") -> str:
        """
        Make interval for the request.
        Usage of the interval parameter:
        1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d, 1w, 1M

        :param interval: Interval of the data.
        :return: interval
        """
        if interval not in self._INTERVALS.keys():
            raise InvalidIntervalError(
                f"Invalid interval: {interval} is not in {self._INTERVALS.keys()}"
            )
        return self._INTERVALS[interval]

    def validate_symbol(self, symbol: str = "BTC_USDT") -> str:
        """
        Validate symbol format.
        :param symbol: cryptocurrency symbol.
        :return: symbol
        """
        if symbol in self.get_all_symbols_futures():
            return symbol
        else:
            raise InvalidSymbolFormatError(f"Invalid symbol: {symbol}")

    def get_all_symbols_futures(self) -> list:
        """
        Get all symbols (futures).
        :return: symbols
        """
        response = requests.get(f"{self._MEXC_BASE_URL}/api/v1/contract/risk_reverse")
        if response.status_code == 200:
            data = response.json()
            return [i["symbol"] for i in data["data"]]
        else:
            print(f"[red]Error: {response.status_code}[/red]")

    def _make_url(self):
        """
        Make url for the request.
        :return: url
        """
        return f"{self._MEXC_BASE_URL}/api/v1/contract/kline/{self.validate_symbol()}"

    def _make_destination_dir(self, symbol: str = "BTC_USDT", interval: str = "1m"):
        """
        Make destination directory.
        :param symbol: cryptocurrency symbol.
        :param interval: Interval of the data.
        :return: destination_dir
        """
        return f"{self._destination_dir}/{symbol}/{interval}"

    def execute_download(
        self, symbol: str, start_date: datetime, end_date: datetime, interval: str
    ):
        """
        Execute download.
        Attention
        1. The maximum data in a single request is 2000 pieces. If your choice
        of start/end time and granularity of time results in more than the maximum volume of data in a single
        request, your request will only return 2000 pieces. If you want to get sufficiently fine-grained data over a
        larger time range, you need to make several times requests.
        2. If only the start time is provided, then query the data from the start time to the current system time.
        If only the end time is provided, the 2000 pieces of data closest to the end time are returned.
        If neither start time nor end time is provided, the 2000 pieces of data closest to the current time
        in the system are queried.
        :param symbol: cryptocurrency symbol.
        :param start_date: Start date of the data.
        :param end_date: End date of the data.
        :param interval: Interval of the data.
        """
        max_retries = 5  # Maximum number of retries
        retry_delay = 30  # Seconds to wait before retrying (30 seconds)

        params = {
            "start": int(start_date.timestamp()),
            "end": int(end_date.timestamp()),
            "interval": self._make_interval(interval),
        }
        for attempt in range(max_retries):
            try:
                response = requests.get(self._make_url(), params=params)
                print(
                    f"[green]Success: {self._make_destination_dir(symbol, interval)}/{int(start_date.timestamp())}.csv[/green]"
                )
                if response.status_code == 200:
                    data = response.json()
                    df = pd.DataFrame(data)
                    df["data"] = df["data"].apply(
                        lambda x: x.strip("[]").split(", ") if isinstance(x, str) else x
                    )
                    expanded_data = df["data"].apply(pd.Series).T
                    expanded_data.to_csv(
                        f"{self._make_destination_dir(symbol, interval)}/{int(start_date.timestamp())}.csv",
                        index=False,
                    )
                else:
                    print(f"[red]Error: {response.status_code}[/red]")
                break
            except (requests.ConnectionError, requests.HTTPError) as e:
                print(f"[red]Error: {e}[/red]")
                time.sleep(retry_delay)
                attempt += 1
        else:
            print(f"[red]Error: Failed to download[/red]")

    def download(
        self,
        symbol: str = "BTC_USDT",
        start_date: datetime = None,
        end_date: datetime = None,
        interval: str = "1m",
    ):
        """
        Download data.
        :param symbol: cryptocurrency symbol.
        :param start_date: Start date of the data.
        :param end_date: End date of the data.
        :param interval: Interval of the data.
        """
        self.validate_symbol(symbol)

        if not os.path.exists(self._make_destination_dir(symbol, interval)):
            os.makedirs(self._make_destination_dir(symbol, interval))

        # 全てのデータを取得する場合
        if start_date is None and end_date is None:
            init_start_date = datetime(2020, 1, 1)
            init_end_date = datetime.now()
            # 2000分ずつデータを取得する
            while True:
                step_time = 2000 * self._INTERVALS_MINUTES[interval]
                # 2秒で20回までの制限があるので、それを考慮する
                if init_start_date + timedelta(minutes=step_time) < init_end_date:
                    # 存在確認
                    if os.path.exists(
                        f"{self._make_destination_dir(symbol, interval)}/{int(init_start_date.timestamp())}.csv"
                    ):
                        print(
                            f"[yellow]Skip: {self._make_destination_dir(symbol, interval)}/{int(init_start_date.timestamp())}.csv[/yellow]"
                        )
                        init_start_date = init_start_date + timedelta(minutes=step_time)
                        continue
                    self.execute_download(
                        symbol,
                        init_start_date,
                        init_start_date + timedelta(minutes=step_time),
                        interval,
                    )
                    init_start_date = init_start_date + timedelta(minutes=step_time)
                    time.sleep(0.1)
                else:
                    self.execute_download(
                        symbol, init_start_date, init_end_date, interval
                    )
                    break

        # all.csvが1日以内に作成されている場合はpassする
        # そうでない場合は削除して作り直す
        if os.path.exists(f"{self._make_destination_dir(symbol, interval)}/all.csv"):
            if datetime.fromtimestamp(
                os.path.getmtime(
                    f"{self._make_destination_dir(symbol, interval)}/all.csv"
                )
            ) > datetime.now() - timedelta(days=1):
                print(
                    f"[yellow]Skip: {self._make_destination_dir(symbol, interval)}/all.csv[/yellow]"
                )
                return
            else:
                os.remove(f"{self._make_destination_dir(symbol, interval)}/all.csv")

        # データを結合する
        df = pd.DataFrame()
        for file in os.listdir(self._make_destination_dir(symbol, interval)):
            print(f"[green]Concat: {file}[/green]")
            df = pd.concat(
                [
                    df,
                    pd.read_csv(
                        f"{self._make_destination_dir(symbol, interval)}/{file}"
                    ),
                ]
            )
        # 時間でソートする
        df.sort_values(by="time", inplace=True)

        # ヘッダーを設定
        df.to_csv(
            f"{self._make_destination_dir(symbol, interval)}/all.csv", index=False
        )

    def download_all(self, interval: str = "1m"):
        """
        Download all data.
        :param interval: Interval of the data.
        """
        for symbol in track(
            self.get_all_symbols_futures(),
            description="Downloading...",
            total=len(self.get_all_symbols_futures()),
        ):
            print(f"[green]Downloading: {symbol}[/green]")
            self.download(symbol, None, None, interval)
