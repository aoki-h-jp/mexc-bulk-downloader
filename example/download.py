from datetime import datetime

from mexc_bulk_downloader.downloader import MexcBulkDownloader

downloader = MexcBulkDownloader(destination_dir=".")
start_time = datetime(2023, 1, 1)
end_time = datetime(2023, 2, 1)

# Download
downloader.download(
    symbol="BTC_USDT", start_date=start_time, end_date=end_time, interval="1m"
)

# interval = 1h
downloader.download(
    symbol="BTC_USDT", start_date=start_time, end_date=end_time, interval="1h"
)
