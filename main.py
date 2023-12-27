from mexc_bulk_downloader.downloader import MexcBulkDownloader

mexc = MexcBulkDownloader(destination_dir="/mnt/f")
mexc.download_all("1m")
