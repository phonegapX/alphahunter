# -*- coding: utf-8 -*-
from pymongo import MongoClient

# MONGO_POOL = MongoClient(host="192.168.1.110",
MONGO_POOL = MongoClient(host="192.168.0.101",
                         maxPoolSize=10,
                         socketTimeoutMS=1000,  # warn:单位毫秒
                         connectTimeoutMS=1000,
                         socketKeepAlive=True,
                         w="majority",
                         j=True,
                         authSource="admin",
                         username="admin",
                         password="admin")


def get_mongo_conn(db="test"):
    """
    use me like this:

    collection = get_mongo_conn()["collection_name"]
    collection.insert({})
    """
    return MONGO_POOL[db]
