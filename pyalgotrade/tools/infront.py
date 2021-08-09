"""
.. moduleauthor:: Carl Westman <carl@alfalyze.com>
"""
import six

from InfrontConnect import infront
from pyalgotrade import bar
from pyalgotrade.barfeed import csvfeed
import pyalgotrade.logger
import os


def format_ticker_from_path(path_str):
    return path_str.split("/")[-1].split(".")[0]


def format_ticker_from_infront_code(infront_code):
    return infront_code.split(":")[-1]


def download_csv(tickers, start_date, end_date, user, password, fields=None, storage=""):
    if fields is None:
        fields = ["open", "high", "low", "last", "bid", "ask", "volume", "turnover"]

    infront.InfrontConnect(user, password)
    raw_data = infront.GetHistory(tickers=tickers, fields=fields,
                                  start_date=start_date.strftime("%Y-%m-%d"),
                                  end_date=end_date.strftime("%Y-%m-%d"))
    ret_list = []
    for ticker in raw_data.keys():
        file_name = storage + ticker + ".csv"
        raw_data[ticker].to_csv(file_name)
        ret_list.append(file_name)

    return ret_list


def build_feed(tickers=[], start_date=None, end_date=None,
               storage="data/", user=None, password=None,
               column_names=None, date_time_format=None,
               force_dowload=False):
    if column_names is None :
        column_names = {
            "datetime": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "last",
            "volume": "volume",
            "adj_close": "last",}

    logger = pyalgotrade.logger.getLogger("quandl")
    ret = csvfeed.GenericBarFeed(bar.Frequency.DAY, timezone=None)

    if date_time_format is None:
        ret.setDateTimeFormat("%Y-%m-%d")
    else:
        ret.setDateTimeFormat(date_time_format)

    # Additional column names.
    for col, name in six.iteritems(column_names):
        ret.setColumnName(col, name)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    files_in_storage = os.listdir(storage)
    available_file = list(map(format_ticker_from_path, files_in_storage))
    required_files = list(map(format_ticker_from_infront_code, tickers))
    all_files = len(available_file) == len(required_files)

    for file in available_file:
        if file not in required_files:
            all_files = False

    if not all_files or force_dowload:
        logger.info("Downloading data for tickers %s from Infront" % required_files)
        files_in_storage = download_csv(tickers, start_date, end_date, user, password, storage=storage)

    for file in files_in_storage:
        ticker = format_ticker_from_path(file)
        ret.addBarsFromCSV(ticker, file)

    return ret
