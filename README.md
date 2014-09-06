bulk-download-quandl
====================

Quick and dirty script to download US stock ticker data (OHLCV) from Quandl, using Python asyncio.

This script downloads the entire [Quandl WIKI dataset](http://www.quandl.com/WIKI) which is in public domain and at the time of this writing contains all US stock data (NYSE and NASDAQ) in daily OHLCV.

The script downloads the data to separate CSV files in a specified folder. It does not re-download the data for a symbol if it is already downloaded, which means that you can stop the script halfway and it knows when to restart. To get the latest daily data, simply delete the file(s) and re-run the script.

The download can take quite some time (a few hours) to complete depending on your settings and network speed.

Requirements
============

* Python 3 with asyncio (3.4)
* aiohttp
* pandas

How to run
==========

1. Copy settings.py.sample to settings.py and put in your configurations.
2. Run the script by `python bulkdlquandl.py`

Why asyncio?
============

I was just playing around with it and happened to need to write this. There is no particular advantage in using asyncio (vs. threads) in such a small scale.

License
=======

MIT License.

