from setuptools import setup

setup(
    name="mexc-bulk-downloader",
    version="1.0.1",
    description="Python library to efficiently and concurrently download historical data files from MEXC. Supports all asset types (spot, USDT-M) and all data frequencies.",
    install_requires=[
        "requests",
        "rich",
        "pytest",
        "pandas",
    ],
    author="aoki-h-jp",
    author_email="aoki.hirotaka.biz@gmail.com",
    license="MIT",
    packages=["mexc_bulk_downloader"],
)
