# -*- coding:utf-8 -*-

"""
Mongodb async API client.
https://docs.mongodb.org/manual/

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import copy

import motor.motor_asyncio
from bson.objectid import ObjectId
from urllib.parse import quote_plus
from functools import wraps

from quant.utils import tools, logger
from quant.tasks import LoopRunTask, SingleTask


__all__ = ("MongoDB", )


DELETE_FLAG = "delete"  # Delete flag, `True` is deleted, otherwise is not deleted.

class MongoDB(object):
    """ Create a MongoDB connection cursor.

    Args:
        db: DB name.
        collection: Collection name.
    """

    _mongo_client = None
    _connected = False

    @classmethod
    def mongodb_init(cls, host="127.0.0.1", port=27017, username="", password="", dbname="admin"):
        """ Initialize a connection pool for MongoDB.

        Args:
            host: Host for MongoDB server.
            port: Port for MongoDB server.
            username: Username for MongoDB server.
            password: Password for MongoDB server.
            dbname: DB name to connect for, default is `admin`.
        """
        if username and password:
            uri = "mongodb://{username}:{password}@{host}:{port}/{dbname}".format(username=quote_plus(username),
                                                                                  password=quote_plus(password),
                                                                                  host=quote_plus(host),
                                                                                  port=port,
                                                                                  dbname=dbname)
        else:
            uri = "mongodb://{host}:{port}/{dbname}".format(host=host, port=port, dbname=dbname)
        cls._mongo_client = motor.motor_asyncio.AsyncIOMotorClient(uri, connectTimeoutMS=5000, serverSelectionTimeoutMS=5000)
        #LoopRunTask.register(cls._check_connection, 2)
        SingleTask.call_later(cls._check_connection, 2) #模拟串行定时器,避免并发
        logger.info("create mongodb connection pool.")

    @classmethod
    async def _check_connection(cls, *args, **kwargs):
        try:
            ns = await cls._mongo_client.list_database_names()
            if ns and isinstance(ns, list) and "admin" in ns:
                cls._connected = True
        except Exception as e:
            cls._connected = False
            logger.error("mongodb connection ERROR:", e)
        finally:
            SingleTask.call_later(cls._check_connection, 2) #开启下一轮检测

    def __init__(self, db, collection):
        """ Initialize. """
        if self._mongo_client == None:
            raise Exception("mongo_client is None")
        self._db = db
        self._collection = collection
        self._cursor = self._mongo_client[db][collection]

    def new_cursor(self, db, collection):
        """ Generate a new cursor.

        Args:
            db: New db name.
            collection: New collection name.

        Return:
            cursor: New cursor.
        """
        if self._mongo_client == None:
            raise Exception("mongo_client is None")
        cursor = self._mongo_client[db][collection]
        return cursor

    def forestall(fn):
        """
        装饰器函数
        """
        @wraps(fn)
        async def wrap(self, *args, **kwargs):
            if not self._connected:
                return None, Exception("mongodb connection lost")
            try:
                return await fn(self, *args, **kwargs)
            except Exception as e:
                return None, e
        return wrap

    @forestall
    async def get_list(self, spec=None, fields=None, sort=None, skip=0, limit=9999, cursor=None):
        """ Get multiple document list.

        Args:
            spec: Query params, optional. Specifies selection filter using query operators.
                To return all documents in a collection, omit this parameter or pass an empty document ({}).
            fields: projection params, optional. Specifies the fields to return in the documents that match the query
                filter. To return all fields in the matching documents, omit this parameter.
            sort: A Set() document that defines the sort order of the result set. e.g. [("age": 1), ("name": -1)]
            skip: The cursor start point, default is 0.
            limit: The max documents to return, default is 9999.
            cursor: Query cursor, default is `self._cursor`.

        Return:
            datas: Documents.

        NOTE:
            MUST input `limit` params, because pymongo maybe change the return documents's count.
        """
        if not spec:
            spec = {}
        if not sort:
            sort = []
        if not cursor:
            cursor = self._cursor
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        spec[DELETE_FLAG] = {"$ne": True}
        datas = []
        result = cursor.find(spec, fields, sort=sort, skip=skip, limit=limit)
        async for item in result:
            #item["_id"] = str(item["_id"])
            datas.append(item)
        return datas, None

    @forestall
    async def find_one(self, spec=None, fields=None, sort=None, cursor=None):
        """ Get one document.

        Args:
            spec: Query params, optional. Specifies selection filter using query operators.
                To return all documents in a collection, omit this parameter or pass an empty document ({}).
            fields: projection params, optional. Specifies the fields to return in the documents that match the query
                filter. To return all fields in the matching documents, omit this parameter.
            sort: A Set() document that defines the sort order of the result set. e.g. [("age": 1), ("name": -1)]
            cursor: Query cursor, default is `self._cursor`.

        Return:
            data: Document or None.
        """
        data = await self.get_list(spec, fields, sort, limit=1, cursor=cursor)
        if data:
            return data[0], None
        else:
            return None, None

    @forestall
    async def count(self, spec=None, cursor=None):
        """ Counts the number of documents referenced by a cursor.

        Args:
            spec: Query params, optional. Specifies selection filter using query operators.
                To return all documents in a collection, omit this parameter or pass an empty document ({}).
            cursor: Query cursor, default is `self._cursor`.

        Return:
            n: Count for query document.

        """
        if not cursor:
            cursor = self._cursor
        if not spec:
            spec = {}
        spec[DELETE_FLAG] = {"$ne": True}
        n = await cursor.count_documents(spec)
        return n, None

    @forestall
    async def insert(self, docs, cursor=None):
        """ Insert (a) document(s).

        Args:
            docs: Dict or List to be inserted.
            cursor: DB cursor, default is `self._cursor`.

        Return:
            Document id(s) that already inserted, if insert a dict, return a id; if insert a list, return a id list.
        """
        if not cursor:
            cursor = self._cursor
        docs_data = copy.deepcopy(docs)
        is_one = False
        if not isinstance(docs_data, list):
            docs_data = [docs_data]
            is_one = True
        result = await cursor.insert_many(docs_data)
        if is_one:
            return result.inserted_ids[0], None
        else:
            return result.inserted_ids, None

    @forestall
    async def update(self, spec, update_fields, upsert=False, multi=False, cursor=None):
        """ Update (a) document(s).

        Args:
            spec: Query params, optional. Specifies selection filter using query operators.
                To return all documents in a collection, omit this parameter or pass an empty document ({}).
            update_fields: Fields to be updated.
            upsert: If server this document if not exist? True or False.
            multi: Update multiple documents? True or False.
            cursor: Query cursor, default is `self._cursor`.

        Return:
            modified_count: How many documents has been modified.
        """
        if not cursor:
            cursor = self._cursor
        update_fields = copy.deepcopy(update_fields)
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        if not multi:
            result = await cursor.update_one(spec, update_fields, upsert=upsert)
            return result.modified_count, None
        else:
            result = await cursor.update_many(spec, update_fields, upsert=upsert)
            return result.modified_count, None

    @forestall
    async def delete(self, spec, cursor=None):
        """ Soft delete (a) document(s).

        Args:
            spec: Query params. Specifies selection filter using query operators.
            cursor: Query cursor, default is `self._cursor`.

        Return:
            delete_count: How many documents has been deleted.
        """
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        update_fields = {"$set": {DELETE_FLAG: True}}
        delete_count = await self.update(spec, update_fields, multi=True, cursor=cursor)
        return delete_count, None

    @forestall
    async def remove(self, spec, multi=False, cursor=None):
        """ delete (a) document(s) perpetually.

        Args:
            spec: Query params. Specifies selection filter using query operators.
            multi: Delete multiple documents? True or False.
            cursor: Query cursor, default is `self._cursor`.

        Return:
            delete_count: How many documents has been deleted.
        """
        if not cursor:
            cursor = self._cursor
        if not multi:
            result = await cursor.delete_one(spec)
            return result.deleted_count, None
        else:
            result = await cursor.delete_many(spec)
            return result.deleted_count, None

    @forestall
    async def distinct(self, key, spec=None, cursor=None):
        """ Distinct query.

        Args:
            key: Distinct key.
            spec: Query params, optional. Specifies selection filter using query operators.
                To return all documents in a collection, omit this parameter or pass an empty document ({}).
            cursor: Query cursor, default is `self._cursor`.

        Return:
            result: Distinct result.
        """
        if not spec:
            spec = {}
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        result = await cursor.distinct(key, spec)
        return result, None

    @forestall
    async def find_one_and_update(self, spec, update_fields, upsert=False, return_document=False, fields=None, cursor=None):
        """ Find a document and update this document.

        Args:
            spec: Query params.
            update_fields: Fields to be updated.
            upsert: If server this document if not exist? True or False.
            return_document: If return new document? `True` return new document, False return old document.
            fields: The fields to be return.
            cursor: Query cursor, default is `self._cursor`.

        Return:
            result: Document.
        """
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        result = await cursor.find_one_and_update(spec, update_fields, projection=fields, upsert=upsert, return_document=return_document)
        #if result and "_id" in result:
        #    result["_id"] = str(result["_id"])
        return result, None

    @forestall
    async def find_one_and_delete(self, spec, fields=None, cursor=None):
        """ Find a document and soft-delete this document.

        Args:
            spec: Query params.
            fields: The fields to be return.
            cursor: Query cursor, default is `self._cursor`.

        Return:
            result: Document.
        """
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        result = await cursor.find_one_and_delete(spec, projection=fields)
        #if result and "_id" in result:
        #    result["_id"] = str(result["_id"])
        return result, None

    def _convert_id_object(self, origin):
        """ Convert a string id to `ObjectId`.
        """
        if isinstance(origin, str):
            return ObjectId(origin)
        elif isinstance(origin, (list, set)):
            return [ObjectId(item) for item in origin]
        elif isinstance(origin, dict):
            for key, value in origin.items():
                origin[key] = self._convert_id_object(value)
        return origin
