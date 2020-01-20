# -*- coding: utf-8 -*-
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_POOL = AsyncIOMotorClient(host=["49.235.96.185", ], maxPoolSize=12, socketTimeoutMS=10000, connectTimeoutMS=10000,
                                authSource="admin", username="admin", password="admin")


def get_mongo_conn(db="test"):
    """
    use me like this:

    collection = get_mongo_conn()["collection_name"]
    collection.insert({})
    """
    return MONGO_POOL[db]
