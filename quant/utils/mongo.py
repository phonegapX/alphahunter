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

from quant.utils import tools
from quant.utils import logger


__all__ = ("initMongodb", "MongoDBBase", )


MONGO_CONN = None
DELETE_FLAG = "delete"  # Delete flag, `True` is deleted, otherwise is not deleted.


def initMongodb(host="127.0.0.1", port=27017, username="", password="", dbname="admin"):
    """ Initialize a connection pool for MongoDB.

    Args:
        host: Host for MongoDB server.
        port: Port for MongoDB server.
        username: Username for MongoDB server.
        password: Username for MongoDB server.
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
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(uri)
    global MONGO_CONN
    MONGO_CONN = mongo_client
    logger.info("create mongodb connection pool.")


class MongoDBBase(object):
    """ Create a MongoDB connection cursor.

    Args:
        db: DB name.
        collection: Collection name.
    """

    def __init__(self, db, collection):
        """ Initialize. """
        self._db = db
        self._collection = collection
        self._conn = MONGO_CONN
        self._cursor = self._conn[db][collection]

    def new_cursor(self, db, collection):
        """ Generate a new cursor.

        Args:
            db: New db name.
            collection: New collection name.

        Return:
            cursor: New cursor.
        """
        cursor = self._conn[db][collection]
        return cursor

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
            item["_id"] = str(item["_id"])
            datas.append(item)
        return datas

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
            return data[0]
        else:
            return None

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
        return n

    async def insert(self, docs, cursor=None):
        """ Insert (a) document(s).

        Args:
            docs: Dict or List to be inserted.
            cursor: DB cursor, default is `self._cursor`.

        Return:
            ret_ids: Document id(s) that already inserted, if insert a dict, `ret_ids` is a id string; if insert a list,
                `ret_ids` is a id list.
        """
        if not cursor:
            cursor = self._cursor
        docs_data = copy.deepcopy(docs)
        ret_ids = []
        is_one = False
        create_time = tools.get_cur_timestamp_ms()
        if not isinstance(docs_data, list):
            docs_data = [docs_data]
            is_one = True
        for doc in docs_data:
            doc["_id"] = ObjectId()
            doc["create_time"] = create_time
            doc["update_time"] = create_time
            ret_ids.append(str(doc["_id"]))
        cursor.insert_many(docs_data)
        if is_one:
            return ret_ids[0]
        else:
            return ret_ids

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
        set_fields = update_fields.get("$set", {})
        set_fields["update_time"] = tools.get_cur_timestamp_ms()
        update_fields["$set"] = set_fields
        if not multi:
            result = await cursor.update_one(spec, update_fields, upsert=upsert)
            return result.modified_count
        else:
            result = await cursor.update_many(spec, update_fields, upsert=upsert)
            return result.modified_count

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
        return delete_count

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
            return result.deleted_count
        else:
            result = await cursor.delete_many(spec)
            return result.deleted_count

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
        return result

    async def find_one_and_update(self, spec, update_fields, upsert=False, return_document=False, fields=None,
                                  cursor=None):
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
        set_fields = update_fields.get("$set", {})
        set_fields["update_time"] = tools.get_cur_timestamp_ms()
        update_fields["$set"] = set_fields
        result = await cursor.find_one_and_update(spec, update_fields, projection=fields, upsert=upsert,
                                                  return_document=return_document)
        if result and "_id" in result:
            result["_id"] = str(result["_id"])
        return result

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
        if result and "_id" in result:
            result["_id"] = str(result["_id"])
        return result

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
