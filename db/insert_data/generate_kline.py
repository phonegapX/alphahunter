# -*- coding: utf-8 -*-
import math
import json
import logging
import copy

from pymongo import UpdateOne, InsertOne

from models import Trade, Symbol, Exchange, Kline


ONE_DAY = 60 * 60 * 24 * 1000  # 一天毫秒数
LIMIT = 500

RE_PATH = "/home/nijun/Documents/timeout/re/"
error_log = logging.getLogger("error_log")
formatter = logging.Formatter("")
fileHandler = logging.FileHandler("error.log", mode='a')
fileHandler.setFormatter(formatter)
error_log.setLevel(logging.ERROR)
error_log.addHandler(fileHandler)


def main():
    begin_timestamp = 1525104000000  # 开始时间, 2018-05-01 00:00:00.000
    end_timestamp = 1578499200000  # 结束时间 2020-01-09  00:00:00.000
    # end_timestamp = begin_timestamp + ONE_DAY * 40
    for begin_dt in range(begin_timestamp, end_timestamp, ONE_DAY):
        day_loop(begin_dt)


def day_loop(begin_dt):
    # 关注标的
    focus_symbols = Symbol.FOCUS_SYMBOLS
    # 交易所
    exchange = Exchange()
    exchange_cursor = exchange.collection.find()

    for e_document in exchange_cursor:
        exchange_name = e_document["name"]
        for focus_symbol in focus_symbols:
            error_log.error("{} {} start".format(exchange_name, begin_dt))
            # 查询trade数据
            trade = Trade(exchange_name, focus_symbol)
            kline = Kline(exchange_name, focus_symbol)

            # 计算
            klines = calculate(trade, kline, begin_dt)

            # 存储
            insert(kline, begin_dt, klines)


def calculate(trade, kline, begin_timestamp):
    # 查询上一天最后一笔trade数据的trade price
    prev_close_price = query_prev_close_price(begin_timestamp, trade)
    prev_kline = query_prev_kline(kline, begin_timestamp)

    klines = []
    kline_document = {}
    update_flag = True  # 是否使用update_one

    # 按照 kline 时间段划分
    # for begin_dt in range(begin_timestamp, begin_timestamp + kline.interval * 5, kline.interval):
    for begin_dt in range(begin_timestamp, begin_timestamp + ONE_DAY, kline.interval):
        end_dt = begin_dt + kline.interval - 1
        kline_document = {
            "begin_dt": begin_dt,
            "end_dt": end_dt,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "avg_price": 0.0,
            "buy_avg_price": 0.0,
            "sell_avg_price": 0.0,
            "open_avg": 0.0,
            "open_avg_fillna": 0.0,
            "close_avg": 0.0,
            "close_avg_fillna": 0.0,
            "volume": 0.0,
            "amount": 0.0,
            "buy_volume": 0.0,
            "buy_amount": 0.0,
            "sell_volume": 0.0,
            "sell_amount": 0.0,
            "sectional_high": 0.0,
            "sectional_low": 0.0,
            "sectional_volume": 0.0,
            "sectional_amount": 0.0,
            "sectional_avg_price": 0.0,
            "sectional_buy_avg_price": 0.0,
            "sectional_sell_avg_price": 0.0,
            "sectional_book_count": 0,
            "sectional_buy_book_count": 0,
            "sectional_sell_book_count": 0,
            "sectional_buy_volume": 0.0,
            "sectional_buy_amount": 0.0,
            "sectional_sell_volume": 0.0,
            "sectional_sell_amount": 0.0,
            "prev_close_price": prev_close_price,
            "next_price": 0.0,
            "next_price_fillna": 0.0,
            "prev_price": prev_kline.get("close_avg", 0.0),
            "prev_price_fillna": prev_kline.get("close_avg_fillna", 0.0),
            "lead_ret": None,
            "lag_ret": None,
            "lead_ret_fillna": None,
            "lag_ret_fillna": None,
            "usable": False
        }

        trade_cursor = trade.collection.find({"dt": {"$gte": begin_dt, "$lte": end_dt}}).sort("dt")

        trades = [t_document for t_document in trade_cursor]
        if trades:
            kline_document["open"] = trades[0]["tradeprice"]
            kline_document["close"] = trades[-1]["tradeprice"]
            results = handle_documents(trades)

            kline_document["avg_price"] = results["avg_price"]
            kline_document["volume"] = results["sum_volume"]

            kline_document["amount"] = results["sum_amount"]
            kline_document["usable"] = True if results["sum_volume"] > 0 else False

        sell_trades = []
        buy_trades = []
        open_trades = []
        close_trades = []
        tradeprices = []

        duration = kline.interval * 0.2

        for t_document in trades:
            tradeprices.append(t_document["tradeprice"])

            if t_document["direction"] == "buy":
                buy_trades.append(t_document)
            else:
                sell_trades.append(t_document)

            if t_document["tradedt"] - begin_dt < duration:
                open_trades.append(t_document)
            elif (begin_dt + kline.interval) - t_document["tradedt"] <= duration:
                close_trades.append(t_document)

        high = max(tradeprices) if tradeprices else 0.0
        low = min(tradeprices) if tradeprices else 0.0
        kline_document["high"] = high
        kline_document["low"] = low

        kline_document["book_count"] = len(trades)
        kline_document["buy_book_count"] = len(buy_trades)
        kline_document["sell_book_count"] = len(sell_trades)

        # 主买
        if buy_trades:
            results = handle_documents(buy_trades)
            kline_document["buy_avg_price"] = results["avg_price"]
            kline_document["buy_volume"] = results["sum_volume"]
            kline_document["buy_amount"] = results["sum_amount"]

        # 主卖
        if sell_trades:
            results = handle_documents(sell_trades)
            kline_document["sell_avg_price"] = results["avg_price"]
            kline_document["sell_volume"] = results["sum_volume"]
            kline_document["sell_amount"] = results["sum_amount"]

        kline_document["sectional_high"] = max([prev_kline.get("sectional_high", 0.0), high])
        prev_kline_sectional_low = prev_kline.get("sectional_low", 0.0)
        min_low = min([low, prev_kline_sectional_low])
        kline_document["sectional_low"] = min_low if min_low else max([low, prev_kline_sectional_low])

        sectional_volume = prev_kline.get("sectional_volume", 0.0) + kline_document["volume"]
        sectional_amount = prev_kline.get("sectional_amount", 0.0) + kline_document["amount"]
        kline_document["sectional_volume"] = sectional_volume
        kline_document["sectional_amount"] = sectional_amount
        kline_document["sectional_avg_price"] = sectional_amount / sectional_volume if sectional_amount else 0.0
        kline_document["sectional_book_count"] = prev_kline.get("sectional_book_count", 0.0) + \
            kline_document["book_count"]

        sectional_buy_volume = prev_kline.get("sectional_buy_volume", 0.0) + kline_document["buy_volume"]
        sectional_buy_amount = prev_kline.get("sectional_buy_amount", 0.0) + kline_document["buy_amount"]
        kline_document["sectional_buy_volume"] = sectional_buy_volume
        kline_document["sectional_buy_amount"] = sectional_buy_amount
        kline_document["sectional_buy_avg_price"] = sectional_buy_amount / sectional_buy_volume \
            if sectional_buy_amount else 0.0
        kline_document["sectional_buy_book_count"] = prev_kline.get("sectional_buy_book_count", 0.0) + \
            kline_document["buy_book_count"]

        sectional_sell_volume = prev_kline.get("sectional_sell_volume", 0.0) + kline_document["sell_volume"]
        sectional_sell_amount = prev_kline.get("sectional_sell_amount", 0.0) + kline_document["sell_amount"]
        kline_document["sectional_sell_volume"] = sectional_sell_volume
        kline_document["sectional_sell_amount"] = sectional_sell_amount
        kline_document["sectional_sell_avg_price"] = \
            sectional_sell_amount / sectional_sell_volume if sectional_sell_amount else 0.0
        kline_document["sectional_sell_book_count"] = prev_kline.get("sectional_sell_book_count", 0.0) + \
            kline_document["sell_book_count"]

        if open_trades:
            results = handle_documents(open_trades)
            kline_document["open_avg"] = results["avg_price"]

        if close_trades:
            results = handle_documents(close_trades)
            kline_document["close_avg"] = results["avg_price"]

        open_avg = kline_document["open_avg"]
        close_avg = kline_document["close_avg"]
        kline_document["open_avg_fillna"] = open_avg if open_avg else kline_document["open"]
        close_avg_fillna = close_avg if close_avg else kline_document["close"]
        kline_document["close_avg_fillna"] = close_avg_fillna

        kline_document["lag_ret"] = math.log(kline_document["close_avg"] / prev_kline["close_avg"]) \
            if prev_kline["close_avg"] and kline_document["close_avg"] else None
        kline_document["lag_ret_fillna"] = math.log(close_avg_fillna / prev_kline["close_avg_fillna"]) \
            if prev_kline["close_avg_fillna"] and close_avg_fillna else None

        # "lead_ret": 0.0  # math.log(next_price / open_avg),
        # "lead_ret_fillna": 0.0  # math.log(next_price_fillna / open_avg_fillna),
        open_avg = kline_document["open_avg"]
        open_avg_fillna = kline_document["open_avg_fillna"]
        lead_ret = math.log(open_avg / prev_kline["open_avg"]) if prev_kline.get("open_avg", 0.0) and open_avg else None
        lead_ret_fillna = math.log(open_avg_fillna / prev_kline["open_avg_fillna"])\
            if prev_kline.get("open_avg_fillna", 0.0) and open_avg_fillna else None
        prev_kline.update({
            "next_price": open_avg,
            "next_price_fillna": open_avg_fillna if open_avg_fillna else prev_kline["close_avg_fillna"],
            "lead_ret": lead_ret,
            "lead_ret_fillna": lead_ret_fillna,
        })

        # 避免插入两天数据， 所以第一个采用update_one
        if update_flag:
            klines.append(UpdateOne(
                {
                    "begin_dt": begin_dt - kline.interval,
                    "end_dt": end_dt - kline.interval,
                },
                {"$set": prev_kline}
            ))
            update_flag = False
        else:
            klines.append(InsertOne(prev_kline))

        # 替换 prev_kline
        prev_kline = kline_document

    # 加上最后一个没有prev_price 的kline
    klines.append(InsertOne(kline_document))
    return klines


def insert(kline, begin_timestamp, klines):
    for offset in range(0, len(klines), LIMIT):
        try:
            documents = copy.deepcopy(klines[offset: offset + LIMIT])
            result = kline.insert_many(documents)
            print(result.bulk_api_result, begin_timestamp, kline.collection_name)
            message = {
                "result": result.bulk_api_result,
                "begin_timestamp": begin_timestamp,
                "name": kline.collection_name
            }
            error_log.error(json.dumps(message))
        except Exception as e:
            error_log.error("error \n")
            error_log.error(e)
            with open(RE_PATH + kline.collection_name + ".txt", "a") as f:
                f.write(json.dumps(klines[offset: offset + LIMIT]))
                f.write("\n")


def query_prev_close_price(begin_timestamp, trade):
    prev_trade_cursor = trade.collection.find(
        {"dt": {"$gte": begin_timestamp - ONE_DAY, "$lt": begin_timestamp}}).sort("dt", -1)
    prev_trades = [t_document for t_document in prev_trade_cursor]
    prev_close_price = prev_trades[0].get("tradeprice", 0.0) if prev_trades else 0.0

    return prev_close_price


def query_prev_kline(kline, begin_timestamp):
    prev_kline = kline.collection.find_one({
        "begin_dt": begin_timestamp - kline.interval,
    }) or {}
    return {
        "open_avg": prev_kline.get("open_avg", 0.0),
        "open_avg_fillna": prev_kline.get("open_avg_fillna", 0.0),
        "close_avg": prev_kline.get("close_avg", 0.0),
        "close_avg_fillna": prev_kline.get("close_avg_fillna", 0.0),
    }


def handle_documents(documents):
    sum_amount = 0
    sum_volume = 0

    for document in documents:
        sum_amount += document["amount"]
        sum_volume += document["volume"]
    return {
        "avg_price": sum_amount / sum_volume,
        "sum_volume": sum_volume,
        "sum_amount": sum_amount,
    }


if __name__ == '__main__':
    # TODO： 根据传入参数决定生成的kline种类
    main()
