# -*- coding: utf-8 -*-

import argparse
import datetime

from pymongo import UpdateOne

from models import Kline, Trade


ONE_DAY = 60*60*24*1000  #一天毫秒数


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

    kline = Kline(exchange, symbol) #数据库K线表读写
    trade = Trade(exchange, symbol) #数据库逐笔成交表读写
    update_one_day_klines(trade, kline, begin_timestamp)


def update_one_day_klines(trade, kline, begin_timestamp):
    #这里有个注意点,每天最后的成交价不一定等于最后一根K线的收盘价
    #因为不管有没有成交,K线每分钟都会生成一根,如果某一分钟没有成交,k线就是全零,
    #比如最后一分钟没有成交,那么最后一根K线的收盘价就是0,但是最后的成交价是存在的,
    #可能是上一分钟,或者上上一分钟,取决于最后一笔成交的时间.
    yestdy_last_trade_price = query_yestdy_last_trade_price(begin_timestamp, trade) #获取上一天最后的成交价
    yestdy_last_kline = query_prev_kline(kline, begin_timestamp) #获取上一天最后一根K线(因为传入的时间是某一天的起点)
    prev_kline = yestdy_last_kline

    klines = []
    
    #接下来处理一天内的所有K线外加上一天最后一根K线
    for begin_dt in range(begin_timestamp, begin_timestamp + ONE_DAY, kline.interval): #<=(一天开始时间), <(下一天开始时间), 步长一分钟
        end_dt = begin_dt + kline.interval - 1
        trade_cursor = trade.collection.find({"dt": {"$gte": begin_dt, "$lte": end_dt}}).sort("dt") #读取一分钟内的所有逐笔成交
        trades = [t_document for t_document in trade_cursor]

        new_kline, prev_kline = kline.generate_kline_ex(begin_dt, trades, prev_kline, yestdy_last_kline, yestdy_last_trade_price) #合成这一分钟的K线

        if begin_dt == begin_timestamp: #第一次循环中的prev_kline指的是上一天最后一根K线
            if yestdy_last_kline: #同时这根K线存在,所以我们就需要更新数据库中上一天最后一根K线
                klines.append(UpdateOne(
                    {
                        "begin_dt": begin_dt - kline.interval
                    },
                    {"$set": prev_kline},
                    upsert=True
                ))
        else:
            klines.append(UpdateOne(
                {
                    "begin_dt": begin_dt - kline.interval
                },
                {"$set": prev_kline},
                upsert=True
            ))

        #替换prev_kline
        prev_kline = new_kline

    #处理一天内最后一根K线
    #下面这四个字段删掉,不更新,因为它的值为0,而数据库里面相应K线字段正常情况下应该是有值的,
    #如果不删除的话就会把数据库里面本来存在的值更新成0。当然如果因为之前实时采集的数据有问题
    #导致数据库里面这四个值有错误也不怕,因为等本程序处理下一天的K线的时候还是会更新本K线这
    #四个字段,因为对于"下一天"来说本K线就是"上一天最后一根K线"
    del prev_kline["next_price"]
    del prev_kline["next_price_fillna"]
    del prev_kline["lead_ret"]
    del prev_kline["lead_ret_fillna"]
    klines.append(UpdateOne(
        {
            "begin_dt": begin_timestamp + ONE_DAY - kline.interval
        },
        {"$set": prev_kline},
        upsert=True
    ))
    
    #更新数据库
    result = kline.insert_many(klines)
    print(result.bulk_api_result)


def query_yestdy_last_trade_price(begin_timestamp, trade):
    prev_trade_cursor = trade.collection.find({"dt": {"$gte": begin_timestamp - ONE_DAY, "$lt": begin_timestamp}}).sort("dt", -1).limit(1)
    prev_trades = [t_document for t_document in prev_trade_cursor]
    r = prev_trades[0].get("tradeprice", 0.0) if prev_trades else 0.0
    return r


def query_prev_kline(kline, begin_timestamp):
    prev_kline = kline.collection.find_one({"begin_dt": begin_timestamp - kline.interval}) or {}
    return prev_kline


if __name__ == '__main__':
    #main()
    
    kline = Kline("huobi", "btcusdt") #数据库K线表读写
    trade = Trade("huobi", "btcusdt") #数据库逐笔成交表读写
    begin_timestamp = int(datetime.datetime.strptime("2020-05-03", "%Y-%m-%d").timestamp() * 1000)
    for i in range(0, 2):
        ts = begin_timestamp + ONE_DAY*i
        #
        dt = datetime.datetime.fromtimestamp(ts/1000)
        print("当前正在处理 " + dt.strftime('%Y-%m-%d %H:%M:%S') + " 这一天的K线")
        #
        update_one_day_klines(trade, kline, ts)
        #
        print("处理完毕\n")