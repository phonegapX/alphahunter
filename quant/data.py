# -*- coding:utf-8 -*-

"""
Data to db.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.utils import tools
from quant.asset import Asset
from quant.market import Kline
from quant.utils.mongo import MongoDBBase


class KLineData:
    """ Save or fetch kline data via MongoDB.

    Data struct:
        {
            "o": open, # Open price
            "h": high, # Highest price
            "l": low, # Lowest price
            "c": close, # Close price
            "t": timestamp # Millisecond timestamp
        }

    Attributes:
        platform: Exchange platform name.
    """

    def __init__(self, platform):
        """ Initialize object.
        """
        self._db_name = platform  # Db name. => MongoDB db name.
        self._collection = "kline"  # Table name. => MongoDB collection name.
        self._platform = platform
        self._k_to_c = {}   # Kline types cursor for db. e.g. {"BTC/USD": "kline_btc_usd"}
        self._db = MongoDBBase(self._db_name, self._collection)  # db instance

    async def create_new_kline(self, kline: Kline):
        """ Insert kline data to db.

        Args:
            kline: kline object.

        Returns:
            kline_id: Kline id, it's a MongoDB document _id.
        """
        cursor = self._get_kline_cursor_by_symbol(kline.symbol)
        data = {
            "o": kline.open,
            "h": kline.high,
            "l": kline.low,
            "c": kline.close,
            "v": kline.volume,
            "t": kline.timestamp
        }
        kline_id = await self._db.insert(data, cursor=cursor)
        return kline_id

    async def get_kline_at_ts(self, symbol, ts=None):
        """ Get a kline data, you can specific symbol and timestamp.

        Args:
            symbol: Symbol pair, e.g. ETH/BTC.
            ts: Millisecond timestamp. If this param is None, ts will be specific current timestamp.

        Returns:
            result: kline data, dict format. If no any data in db, result is None.
        """
        cursor = self._get_kline_cursor_by_symbol(symbol)
        if ts:
            spec = {"t": {"$lte": ts}}
        else:
            spec = {}
        _sort = [("t", -1)]
        result = await self._db.find_one(spec, sort=_sort, cursor=cursor)
        return result

    async def get_latest_kline_by_symbol(self, symbol):
        """ Get latest kline data by symbol.

        Args:
            symbol: Symbol pair, e.g. ETH/BTC.

        Returns:
            result: kline data, dict format. If no any data in db, result is None.
        """
        cursor = self._get_kline_cursor_by_symbol(symbol)
        sort = [("create_time", -1)]
        result = await self._db.find_one(sort=sort, cursor=cursor)
        return result

    async def get_kline_between_ts(self, symbol, start, end):
        """ Get some kline data between two timestamps.

        Args:
            symbol: Symbol pair, e.g. ETH/BTC.
            start: Millisecond timestamp, the start time you want to specific.
            end: Millisecond timestamp, the end time you want to specific.

        Returns:
            result: kline data, list format. If no any data in db, result is a empty list.
        """
        cursor = self._get_kline_cursor_by_symbol(symbol)
        spec = {
            "t": {
                "$gte": start,
                "$lte": end
            }
        }
        fields = {
            "create_time": 0,
            "update_time": 0
        }
        _sort = [("t", 1)]
        datas = await self._db.get_list(spec, fields=fields, sort=_sort, cursor=cursor)
        return datas

    def _get_kline_cursor_by_symbol(self, symbol):
        """ Get a cursor name by symbol, we will convert a symbol name to a collection name.
            e.g. ETH/BTC => kline_eth_btc

        Args:
            symbol: Symbol pair, e.g. ETH/BTC.

        Returns:
            cursor: DB query cursor name.
        """
        cursor = self._k_to_c.get(symbol)
        if not cursor:
            s = symbol.replace("/", "").replace("-", "")
            collection = "kline_{}".format(s)
            cursor = self._db.new_cursor(self._db_name, collection)
            self._k_to_c[symbol] = cursor
        return cursor


class AssetData:
    """ Save or fetch asset data via MongoDB.

    Data struct:
        {
            "platform": "binance", # Exchange platform name.
            "account": "test@gmail.com", # Account name.
            "timestamp": 1234567890, # Millisecond timestamp.
            "assets": {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... } # Currency details for BTC...
        }
    """

    DB = "asset"  # db name
    COLLECTION = "asset"  # collection name

    def __init__(self):
        """Initialize object."""
        self._db = MongoDBBase(self.DB, self.COLLECTION)  # db instance

    async def create_new_asset(self, asset: Asset):
        """ Insert asset data to db.

        Args:
            asset: Asset object.

        Returns:
            asset_id: Asset id, it's a MongoDB document _id.
        """
        d = {
            "platform": asset.platform,
            "account": asset.account,
            "timestamp": asset.timestamp,
            "assets": asset.assets
        }
        asset_id = await self._db.insert(d)
        return asset_id

    async def create_new_assets(self, *assets: Asset):
        """ Insert asset data list to db.

        Args:
            assets: One or multiple Asset object.

        Returns:
            asset_ids: Asset id list, every item is a MongoDB document _id.
        """
        docs = []
        for asset in assets:
            d = {
                "platform": asset.platform,
                "account": asset.account,
                "timestamp": asset.timestamp,
                "assets": asset.assets
            }
            docs.append(d)
        asset_ids = await self._db.insert(docs)
        return asset_ids

    async def update_asset(self, asset: Asset):
        """ Update asset data.

        Args:
            asset: Asset object.

        Returns:
            count: How many documents have been updated.
        """
        spec = {
            "platform": asset.platform,
            "account": asset.account
        }
        update_fields = {"$set": {"timestamp": asset.timestamp, "assets": asset.assets}}
        count = await self._db.update(spec, update_fields=update_fields, upsert=True)
        return count

    async def get_latest_asset(self, platform, account):
        """ Get latest asset data.

        Args:
            platform: Exchange platform name. e.g. binance/bitmex/okex
            account: Account name. e.g. test@gmail.com

        Returns:
            asset: Asset data, e.g. {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        """
        spec = {
            "platform": platform,
            "account": account
        }
        _sort = [("timestamp", -1)]
        fields = {
            "create_time": 0,
            "update_time": 0
        }
        asset = await self._db.find_one(spec, sort=_sort, fields=fields)
        if asset:
            del asset["_id"]
        return asset


class AssetSnapshotData(AssetData):
    """ Save or fetch asset snapshot data via MongoDB.

    Data struct:
        {
            "platform": "binance", # Exchange platform name.
            "account": "test@gmail.com", # Account name.
            "timestamp": 1234567890, # Millisecond timestamp.
            "BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"},  # Currency details for BTC.
            "ETH": { ... },
            ...
        }
    """

    DB = "asset"  # db name
    COLLECTION = "snapshot"  # collection name

    async def get_asset_snapshot(self, platform, account, start=None, end=None):
        """ Get asset snapshot data from db.

        Args:
            platform: Exchange platform name. e.g. binance/bitmex/okex
            account: Account name. e.g. test@gmail.com
            start: Start time, Millisecond timestamp, default is a day ago.
            end: End time, Millisecond timestamp, default is current timestamp.

        Returns:
            datas: Asset data list. e.g. [{"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }, ... ]
        """
        if not end:
            end = tools.get_cur_timestamp_ms()  # Current timestamp
        if not start:
            start = end - 60 * 60 * 1000 * 24  # A day ago.
        spec = {
            "platform": platform,
            "account": account,
            "timestamp": {
                "$gte": start,
                "$lte": end
            }
        }
        fields = {
            "platform": 0,
            "account": 0,
            "update_time": 0
        }
        datas = await self._db.get_list(spec, fields=fields)
        return datas


class OrderData:
    """ Save or fetch order data via MongoDB.

    Data struct:
        {
            "p": order.platform,
            "a": order.account,
            "s": order.strategy,
            "S": order.symbol,
            "n": order.order_no,
            "A": order.action,
            "t": order.order_type,
            "st": order.status,
            "pr": order.price,
            "ap": order.avg_price,
            "q": order.quantity,
            "r": order.remain,
            "T": order.trade_type,
            "ct": order.ctime,
            "ut": order.utime
        }
        All the fields are defined in Order module.
    """

    def __init__(self):
        """Initialize object."""
        self._db = "order"  # db name
        self._collection = "order"  # collection name
        self._db = MongoDBBase(self._db, self._collection)  # db instance

    async def create_new_order(self, order):
        """ Insert order data to db.

        Args:
            order: Order object.

        Returns:
            order_id: order data id, it's a MongoDB document _id.
        """
        data = {
            "p": order.platform,
            "a": order.account,
            "s": order.strategy,
            "S": order.symbol,
            "n": order.order_no,
            "A": order.action,
            "t": order.order_type,
            "st": order.status,
            "pr": order.price,
            "ap": order.avg_price,
            "q": order.quantity,
            "r": order.remain,
            "T": order.trade_type,
            "ct": order.ctime,
            "ut": order.utime
        }
        order_id = await self._db.insert(data)
        return order_id

    async def get_order_by_no(self, platform, order_no):
        """ Get a order by order no.

        Args:
            platform: Exchange platform name.
            order_no: order no.

        Returns:
            data: order data, dict format.
        """
        spec = {
            "p": platform,
            "n": order_no
        }
        data = await self._db.find_one(spec)
        return data

    async def update_order_infos(self, order):
        """ Update order information.

        Args:
            order: Order object.

        Returns:
            count: How many documents have been updated.
        """
        spec = {
            "p": order.platform,
            "n": order.order_no
        }
        update_fields = {
            "s": order.status,
            "r": order.remain
        }
        count = await self._db.update(spec, update_fields={"$set": update_fields})
        return count

    async def get_latest_order(self, platform, symbol):
        """ Get a latest order data.

        Args:
            platform: Exchange platform name.
            symbol: Symbol name.

        Return:
            data: order data, dict format.
        """
        spec = {
            "p": platform,
            "S": symbol
        }
        _sort = [("ut", -1)]
        data = await self._db.find_one(spec, sort=_sort)
        return data
