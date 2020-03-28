# -*- coding: utf-8 -*-
import os
import json
import copy
from models import OrderBook, Trade

RE_PATH = "/Users/nanqiang/Documents/work/re/"


def main():
    # 读取文件
    file_names = os.listdir(RE_PATH)
    # file_names.remove(".DS_Store")
    for file_name in file_names:

        names = file_name.split("_")
        _type = names[1]
        print(names)
        exchange_name = names[2]
        symbol_name = names[3].replace(".txt", "")
        if _type == "trade":
            obj = Trade(exchange_name, symbol_name)
        elif _type == "orderbook":
            obj = OrderBook(exchange_name, symbol_name)
        # 插入数据
        with open(RE_PATH + file_name) as f:
            for line in f.readlines():
                line = json.loads(line)
                documents = copy.deepcopy(line)
                for document in documents:
                    try:
                        obj.collection.replace_one(filter=document, replacement=document, upsert=True)
                    except Exception:
                        with open(obj.collection_name + ".txt", "a") as f:
                            f.write(json.dumps(document))
                            f.write("\n")


if __name__ == '__main__':
    main()
