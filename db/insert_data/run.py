# -*- coding: utf-8 -*-
import os
import shutil
import pandas
import time
import datetime
import json
import logging
import copy

from zipfile import ZipFile, is_zipfile

from models import Symbol, OrderBook, Trade


PATH = "/home/nijun/Documents/huobi"  # 文件存放地址
RE_PATH = "/home/nijun/Documents/timeout/re/"
LIMIT = 1000  # 批量插入条数限制


# buy_or_sell 转换
DIRECTION = {
    "b": "BUY",
    "s": "SELL",
}

# exchange name 转换
EXCHANGE_NAME = {
    "huobipro": "huobi",
}
error_log = logging.getLogger("error_log")
formatter = logging.Formatter("")
fileHandler = logging.FileHandler("/home/nijun/Documents/timeout/error.log", mode='a')
fileHandler.setFormatter(formatter)
error_log.setLevel(logging.ERROR)
error_log.addHandler(fileHandler)


def main():
    zip_paths = get_zips(PATH)

    for zip_path in zip_paths:
        zip_file = ZipFile(zip_path)
        # 如果压缩包里面是以.csv结尾的文件, 则表明已经到最后一层目录
        zip_info_list = zip_file.infolist()
        if zip_info_list and zip_info_list[0].filename.endswith(".csv"):
            read_file(zip_file)
        else:
            handler_not_finally_path(zip_file)
    print("finish", datetime.datetime.now())


def get_zips(dir_path):
    """
    获取目录下的所有压缩文件
    """
    if is_zipfile(dir_path):
        return [dir_path]
    zip_paths = []
    names = get_names(dir_path)
    for name in names:
        next_path = os.path.join(dir_path, name)
        zip_paths.extend(get_zips(next_path))
    return zip_paths


def handler_not_finally_path(zip_file):
    zip_info_list = zip_file.infolist()
    if zip_info_list and zip_info_list[0].filename.endswith(".csv"):
        return

    # 解压
    extra_path = zip_file.filename.strip(".zip")
    zip_file.extractall(path=extra_path)

    # 捕获异常, 保证清理解压文件
    try:
        # 暂时符合目前新旧路径格式, 暂时不写递归, 如果之后出现别的情况, 再
        zip_paths = get_zips(extra_path)
        for zip_path in zip_paths:
            zip_file = ZipFile(zip_path)
            read_file(zip_file)
    except Exception as e:
        # TODO: 发送钉钉提醒, 或者微信提醒
        logging.error(e)
    finally:
        if os.path.exists(extra_path):
            # 删除文件
            shutil.rmtree(extra_path)
            print("*******删除成功*******")


def read_file(zip_file):
    file_names = ignore(
        [zip_info.filename for zip_info in zip_file.infolist()])
    for file_name in file_names:
        # TODO: 添加对file_name split结果的校验
        file_name_split = file_name.split("_")

        # 是否存储
        symbol_name = file_name_split[1].lower()
        focus_symbol = Symbol.is_focus(symbol_name)
        if not focus_symbol:
            continue
        print(file_name)

        # exchange 转换
        exchange_name = file_name_split[0].lower()
        exchange_name = EXCHANGE_NAME.get(exchange_name, exchange_name)

        # 插入symbol
        insert_symbol(exchange_name, symbol_name)

        with zip_file.open(file_name) as f:
            # 跳过第一行. 第一行为联系信息
            df = pandas.read_csv(f, skiprows=1)

            # trade 数据
            if "TICK" in file_name:
                handle_trade(exchange_name, symbol_name, df)

            # order book 数据
            elif "ORDER" in file_name:
                handle_order_book(exchange_name, symbol_name, df)


def handle_trade(exchange_name, symbol_name, df):
    """
    binance example:
    aggregate_ID  time                    price   amount  buy_or_sell first_trade_ID  last_trade_ID
    1074495       2020-01-04 00:00:01.751 222.38  0.14614    b        1213680          1213680
    """
    # huobi / binance
    # df.rename(columns={"exchange_time": "tradedt", "time": "tradedt", "price": "tradeprice", "amount":
    #                    "volume", "buy_or_sell": "direction"}, inplace=True)

    # okex
    df.rename(columns={"server_time": "tradedt", "time": "tradedt", "price": "tradeprice", "amount":
                       "volume", "buy_or_sell": "direction"}, inplace=True)

    df["direction"] = df.apply(direction_change, axis=1)
    df["tradedt"] = df.apply(str_2_timestamp, axis=1)
    df.insert(2, "dt", df["tradedt"])
    df.insert(5, "amount", df["volume"]*df["tradeprice"])

    # 删除多余行
    tolist = df.columns.values.tolist()
    drop_columns = list(set(tolist) - set(Trade.COLUMNS))
    df.drop(drop_columns, axis=1, inplace=True)

    trade = Trade(exchange_name, symbol_name)
    rows = df.to_dict('records')

    for skip in range(0, len(rows), LIMIT):
        documents = copy.deepcopy(rows[skip: skip + LIMIT])
        try:
            trade.collection.insert_many(documents)
        except Exception as e:
            error_log.error(e)
            with open(RE_PATH + trade.collection_name + ".txt", "a") as f:
                f.write(json.dumps(rows[skip: skip + LIMIT]))
                f.write("\n")


def handle_order_book(exchange_name, symbol_name, df):
    """
    example
    lastUpdateId	server_time	         	buy_1_price	... buy_20_price    sell_1_price ... sell_20_price    buy_1_amount ... buy_20_amount    sell_1_amount ... sell_20_amount
    23389043	2020-01-01 00:00:11.732970	7137.91000000	7135.44000000   7150.00000000    7900.00000000    0.00203000       0.00728600       3.38056700        sell_20_amount
    """  # NOQA
    columns = ["lastUpdateId", "dt", ]
    prefixs = ["bidprice", "askprice", "bidsize", "asksize"]

    # HUOBIPRO, BINANCE 都是去掉前两行后为 "bidprice", "askprice", "bidsize", "asksize", 只不过HUOBIPRO 为150档
    level = int((df.shape[1] - 2) / 4)
    for prefix in prefixs:
        columns.extend(["{prefix}{level}".format(
            prefix=prefix, level=level) for level in range(1, level+1)])
    df.columns = columns

    # 删除多余行
    tolist = df.columns.values.tolist()
    drop_columns = list(set(tolist) - set(OrderBook.COLUMNS))
    df.drop(drop_columns, axis=1, inplace=True)

    # 转换时间
    df["dt"] = df.apply(str_2_timestamp, axis=1)

    order_book = OrderBook(exchange_name, symbol_name)
    rows = df.to_dict('records')

    for skip in range(0, len(rows), LIMIT):
        documents = copy.deepcopy(rows[skip: skip + LIMIT])
        try:
            order_book.collection.insert_many(documents)
        except Exception as e:
            error_log.error(e)
            with open(RE_PATH + order_book.collection_name + ".txt", "a") as f:
                f.write(json.dumps(rows[skip: skip + LIMIT]))
                f.write("\n")


def direction_change(series):
    direction = series.get("direction")
    return DIRECTION[direction]


def str_2_timestamp(series):
    utc_date_str = series.get("tradedt") or series.get("dt")
    utc_d = datetime.datetime.strptime(utc_date_str, "%Y-%m-%d %H:%M:%S.%f")
    # 转换为本地(东八区)时间, 加8小时
    local_d = utc_d + datetime.timedelta(hours=8)
    t = local_d.timetuple()
    timestamp = int(time.mktime(t)) * 1000 + round(local_d.microsecond / 1000)
    return timestamp


def insert_exchange(name):
    """
    单独定义列表手动存储, 或者可以保证所有交易所路径统一, 可以跟随trade等数据同步存储
    """
    t_exchange = get_mongo_conn()["t_exchange"]  # NOQA
    pass


def insert_symbol(exchange_name, symbol_name):
    """
    目前只考虑部分交易对
    """
    t_symbol = Symbol().collection
    symbol = {"exchange": exchange_name, "name": symbol_name,
              "research_usable": True, "trade_usable": True}
    t_symbol.replace_one(filter=symbol, replacement=symbol, upsert=True)


def get_names(dir_path):
    names = ignore(os.listdir(dir_path))
    return names


def ignore(names):
    ignore_names = {".DS_Store", "__MACOSX"}
    names = set(names) - ignore_names
    return list(names)


if __name__ == '__main__':
    main()
