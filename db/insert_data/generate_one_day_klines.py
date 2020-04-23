# -*- coding: utf-8 -*-
import argparse
import datetime

from pymongo import UpdateOne, InsertOne

from models import Kline, Trade


ONE_DAY = 60 * 60 * 24 * 1000  # 一天毫秒数
LIMIT = 500


def main():
    """
    """
    parser = argparse.ArgumentParser(description="生成某一天kline")
    parser.add_argument("--day", "-d", help="日期")
    parser.add_argument("--exchange", "-e", help="交易所名称", type=str)
    parser.add_argument("--symbol", "-s", help="标的", type=str)

    args = parser.parse_args()
    day = args.day
    exchange = args.exchange
    symbol = args.symbol
    if not day or not exchange or not symbol:
        print("Invalid args!!!")
        print("example:\n\npython db/insert_data/generate_last_day_klines.py -d 2012-12-2 -e huobi -s btcusdt")
        return

    begin_timestamp = int(datetime.datetime.strptime(day, "%Y-%m-%d").timestamp() * 1000)

    kline = Kline(exchange, symbol)
    trade = Trade(exchange, symbol)
    calculate(trade, kline, begin_timestamp)


def calculate(trade, kline, begin_timestamp):
    # 查询上一天最后一笔trade数据的trade price
    prev_close_price = query_prev_close_price(begin_timestamp, trade)
    yestdy_last_kline = query_prev_kline(kline, begin_timestamp)
    prev_kline = yestdy_last_kline

    klines = []
    new_kline = {}
    update_flag = True  # 是否使用update_one

    # 按照 kline 时间段划分
    # for begin_dt in range(begin_timestamp, begin_timestamp + kline.interval * 5, kline.interval):
    for begin_dt in range(begin_timestamp, begin_timestamp + ONE_DAY, kline.interval):
        end_dt = begin_dt + kline.interval - 1
        trade_cursor = trade.collection.find({"dt": {"$gte": begin_dt, "$lte": end_dt}}).sort("dt")
        trades = [t_document for t_document in trade_cursor]

        new_kline, prev_kline = kline.generate_kline_ex(
            begin_dt, trades, prev_kline, yestdy_last_kline, prev_close_price
        )

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
        prev_kline = new_kline

    # 加上最后一个没有prev_price 的kline
    klines.append(InsertOne(new_kline))
    return klines


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


if __name__ == '__main__':
    main()
