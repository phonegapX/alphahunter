# -*- coding:utf-8 -*-

"""
OKEx Trade module.
https://www.okex.me/docs/zh/
备注: OKEX网站界面显示的符号格式类似BTC/USDT,而API实际使用的符号格式却是类似BTC-USDT,这里统一使用API所用格式

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import json
import hmac
import copy
import zlib
import base64
import urllib
import hashlib
import datetime
from urllib import parse
from urllib.parse import urljoin
from collections import defaultdict, deque
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional, Any

from quant.gateway import ExchangeGateway
from quant.state import State
from quant.utils import tools, logger
from quant.const import MARKET_TYPE_KLINE
from quant.order import Order, Fill, SymbolInfo
from quant.position import Position
from quant.asset import Asset
from quant.tasks import SingleTask, LoopRunTask
from quant.utils.websocket import Websocket
from quant.utils.decorator import async_method_locker
from quant.utils.http_client import AsyncHttpRequests
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET, ORDER_TYPE_IOC
from quant.order import LIQUIDITY_TYPE_MAKER, LIQUIDITY_TYPE_TAKER
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED
from quant.market import Kline, Orderbook, Trade, Ticker


__all__ = ("OKExRestAPI", "OKExTrader", )


class OKExRestAPI:
    """ OKEx REST API client.

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY Passphrase.
    """

    def __init__(self, host, access_key, secret_key, passphrase):
        """initialize."""
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key
        self._passphrase = passphrase

    async def get_symbols_info(self):
        """ 获取所有交易对基础信息

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        result, error = await self.request("GET", "/api/spot/v3/instruments")
        return result, error

    async def get_account_balance(self):
        """ Get account asset information.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        result, error = await self.request("GET", "/api/spot/v3/accounts", auth=True)
        return result, error

    async def create_order(self, action, symbol, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ Create an order.
        Args:
            action: Action type, `BUY` or `SELL`.
            symbol: Trading pair, e.g. BTC-USDT.
            price: Order price.
            quantity: Order quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        info = {
            "side": "buy" if action == ORDER_ACTION_BUY else "sell",
            "instrument_id": symbol,
            "margin_trading": 1
        }
        if order_type == ORDER_TYPE_LIMIT:
            info["type"] = "limit"
            info["price"] = price
            info["size"] = quantity
        elif order_type == ORDER_TYPE_MARKET:
            info["type"] = "market"
            if action == ORDER_ACTION_BUY:
                info["notional"] = quantity  # 买金额.
            else:
                info["size"] = quantity  # sell quantity.
        elif order_type == ORDER_TYPE_IOC:
            info["type"] = "limit"
            info["price"] = price
            info["size"] = quantity
            info["order_type"] = "3"
        else:
            logger.error("order_type error! order_type:", order_type, caller=self)
            return None
        result, error = await self.request("POST", "/api/spot/v3/orders", body=info, auth=True)
        return result, error

    async def revoke_order(self, symbol, order_no):
        """ Cancelling an unfilled order.
        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_no: order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        body = {
            "instrument_id": symbol
        }
        uri = "/api/spot/v3/cancel_orders/{order_no}".format(order_no=order_no)
        result, error = await self.request("POST", uri, body=body, auth=True)
        if error:
            return order_no, error
        if result["result"]:
            return order_no, None
        return order_no, result

    async def revoke_orders(self, symbol, order_nos):
        """ Cancelling multiple open orders with order_id，Maximum 10 orders can be cancelled at a time for each
            trading pair.

        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_nos: order IDs.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if len(order_nos) > 10:
            logger.warn("only revoke 10 orders per request!", caller=self)
        body = [
            {
                "instrument_id": symbol,
                "order_ids": order_nos[:10]
            }
        ]
        result, error = await self.request("POST", "/api/spot/v3/cancel_batch_orders", body=body, auth=True)
        return result, error

    async def get_open_orders(self, symbol, limit=100):
        """ Get order details by order ID.

        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            limit: order count to return, max is 100, default is 100.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/spot/v3/orders_pending"
        params = {
            "instrument_id": symbol,
            "limit": limit
        }
        result, error = await self.request("GET", uri, params=params, auth=True)
        return result, error

    async def get_order_status(self, symbol, order_no):
        """ Get order status.
        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_no: order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
            "instrument_id": symbol
        }
        uri = "/api/spot/v3/orders/{order_no}".format(order_no=order_no)
        result, error = await self.request("GET", uri, params=params, auth=True)
        return result, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(self._host, uri)

        if auth:
            time_str = str(time.time())
            timestamp = time_str.split(".")[0] + "." + time_str.split(".")[1][:3]
            if body:
                body = json.dumps(body)
            else:
                body = ""
            message = str(timestamp) + str.upper(method) + uri + str(body)
            mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"), digestmod="sha256")
            d = mac.digest()
            sign = base64.b64encode(d)

            if not headers:
                headers = {}
            headers["Content-Type"] = "application/json"
            headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
            headers["OK-ACCESS-SIGN"] = sign.decode()
            headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
            headers["OK-ACCESS-PASSPHRASE"] = self._passphrase
        _, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10)
        return success, error


class OKExTrader(Websocket, ExchangeGateway):
    """ OKEx Trade module
    """

    def __init__(self, **kwargs):
        """Initialize."""
        self.cb = kwargs["cb"]
        state = None
        
        if kwargs.get("account") and (not kwargs.get("access_key") or not kwargs.get("secret_key")):
            state = State("param access_key or secret_key miss")
        if not kwargs.get("strategy"):
            state = State("param strategy miss")
        if not kwargs.get("symbols"):
            state = State("param symbols miss")
        if not kwargs.get("passphrase"):
            state = State("param passphrase miss")
        if state:
            logger.error(state, caller=self)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return

        self._platform = kwargs["platform"]
        self._symbols = kwargs["symbols"]
        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]

        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]

        self._host = "https://www.okex.me"
        self._wss = "wss://real.okex.me:8443"

        self._order_channel = []
        for sym in self._symbols:
            self._order_channel.append("spot/order:{symbol}".format(symbol=sym))

        url = self._wss + "/ws/v3"
        super(OKExTrader, self).__init__(url, send_hb_interval=5, **kwargs)
        self.heartbeat_msg = "ping"
        
        self._syminfo:DefaultDict[str: Dict[str, Any]] = defaultdict(dict)
        
        self._assets: DefaultDict[str: Dict[str, float]] = defaultdict(lambda: {k: 0.0 for k in {'free', 'locked', 'total'}})

        # Initializing our REST API client.
        self._rest_api = OKExRestAPI(self._host, self._access_key, self._secret_key, self._passphrase)

        if self._account != None:
            self.initialize()

        #市场行情数据
        OKExMarket(**kwargs)

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, *args, **kwargs):
        """ Create an order.

        Args:
            symbol: Trade target
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)
        result, error = await self._rest_api.create_order(action, symbol, price, quantity, order_type)
        if error:
            return None, error
        if not result["result"]:
            return None, result
        return result["order_id"], None

    async def revoke_order(self, symbol, *order_nos):
        """ 撤销订单
        @param symbol 交易对
        @param order_nos 订单号列表，可传入任意多个，如果不传入，那么就撤销所有订单
        备注:关于批量删除订单函数返回值格式,如果函数调用失败了那肯定是return None, error
        如果函数调用成功,但是多个订单有成功有失败的情况,比如输入3个订单id,成功2个,失败1个,那么
        返回值统一都类似: 
        return [(成功订单ID, None),(成功订单ID, None),(失败订单ID, "失败原因")], None
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol
        if len(order_nos) == 0:
            success, error = await self._rest_api.get_open_orders(symbol)
            if error:
                return False, error
            order_nos = []
            for order_info in success:
                order_nos.append(order_info["order_id"])

        if len(order_nos) == 0:
            return [], None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(symbol, order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            """
            {
            "BTC-USDT":[
            {
               "result":true,
                "client_oid":"a123",
                "order_id": "2510832677225473"
             },
             {
               "result":true,
                "client_oid":"a1234",
                "order_id": "2510832677225474"
             }]
            }
            """
            s, e = await self._rest_api.revoke_orders(symbol, order_nos)
            if e:
                return [], e
            for d in s.get(symbol):
                if d["result"]:
                    result.append((d["order_id"], None))
                else:
                    result.append((d["order_id"], d["error_message"]))
            return result, None

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(symbol)
        if error:
            return None, error
        else:
            orders:List[Order] = []
            for order_info in success:
                order = self._convert_order_format(order_info)
                orders.append(order)
            return orders, None

    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_account_balance()
        if error:
            return None, error
        ast = self._convert_asset_format(success)
        return ast, None

    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        raise NotImplementedError

    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        """
        [
        {
            "base_currency":"BTC",
            "instrument_id":"BTC-USDT",
            "min_size":"0.001",
            "quote_currency":"USDT",
            "size_increment":"0.00000001",
            "tick_size":"0.1"
        },
        {
            "base_currency":"OKB", #交易货币币种
            "instrument_id":"OKB-USDT", #币对名称
            "min_size":"1", #最小交易数量
            "quote_currency":"USDT", #计价货币币种
            "size_increment":"0.0001", #交易货币数量精度
            "tick_size":"0.0001" #交易价格精度
        }
        ]
        """
        info = self._syminfo[symbol]
        if not info:
            return None, "Symbol not exist"
        price_tick = float(info["tick_size"])
        size_tick = float(info["size_increment"])
        size_limit = float(info["min_size"])
        value_tick = None #原始数据中没有
        value_limit = None#原始数据中没有
        base_currency = info["base_currency"]
        quote_currency = info["quote_currency"]
        syminfo = SymbolInfo(self._platform, symbol, price_tick, size_tick, size_limit, value_tick, value_limit, base_currency, quote_currency)
        return syminfo, None

    async def invalid_indicate(self, symbol, indicate_type):
        """ update (an) callback function.

        Args:
            symbol: Trade target
            indicate_type: INDICATE_ORDER, INDICATE_ASSET, INDICATE_POSITION

        Returns:
            success: If execute successfully, return True, otherwise it's False.
            error: If execute failed, return error information, otherwise it's None.
        """
        async def _task():
            if indicate_type == INDICATE_ORDER and self.cb.on_order_update_callback:
                success, error = await self.get_orders(symbol)
                if error:
                    state = State("get_orders error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                    SingleTask.run(self.cb.on_state_update_callback, state)
                    return
                for order in success:
                    SingleTask.run(self.cb.on_order_update_callback, order)
            elif indicate_type == INDICATE_ASSET and self.cb.on_asset_update_callback:
                success, error = await self.get_assets()
                if error:
                    state = State("get_assets error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                    SingleTask.run(self.cb.on_state_update_callback, state)
                    return
                SingleTask.run(self.cb.on_asset_update_callback, success)

        if indicate_type == INDICATE_ORDER or indicate_type == INDICATE_ASSET:
            SingleTask.run(_task)
            return True, None
        elif indicate_type == INDICATE_POSITION:
            raise NotImplementedError
        else:
            logger.error("indicate_type error! indicate_type:", indicate_type, caller=self)
            return False, "indicate_type error"

    @property
    def rest_api(self):
        return self._rest_api

    async def connected_callback(self):
        """After websocket connection created successfully, we will send a message to server for authentication."""
        time_str = str(time.time())
        timestamp = time_str.split(".")[0] + "." + time_str.split(".")[1][:3]
        message = timestamp + "GET" + "/users/self/verify"
        mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf8"), digestmod="sha256")
        d = mac.digest()
        signature = base64.b64encode(d).decode()
        data = {
            "op": "login",
            "args": [self._access_key, self._passphrase, timestamp, signature]
        }
        await self.send_json(data)

    async def _auth_success_callback(self):
        """ 授权成功之后回调
        """
        #获取相关符号信息
        """
        [
        {
            "base_currency":"BTC",
            "instrument_id":"BTC-USDT",
            "min_size":"0.001",
            "quote_currency":"USDT",
            "size_increment":"0.00000001",
            "tick_size":"0.1"
        },
        {
            "base_currency":"OKB",
            "instrument_id":"OKB-USDT",
            "min_size":"1",
            "quote_currency":"USDT",
            "size_increment":"0.0001",
            "tick_size":"0.0001"
        }
        ]
        """
        success, error = await self._rest_api.get_symbols_info()
        if error:
            state = State("get_symbols_info error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return
        for info in success:
            self._syminfo[info["instrument_id"]] = info #符号信息一般不变,获取一次保存好,其他地方要用直接从这个变量获取就可以了

        #获取账户余额,更新资产
        """
        [
        {
            "frozen":"0",
            "hold":"0",
            "id": "",
            "currency":"BTC",
            "balance":"0.0049925",
            "available":"0.0049925",
            "holds":"0"
        },
        {
            "frozen":"0",
            "hold":"0",
            "id": "",
            "currency":"USDT",
            "balance":"226.74061435",
            "available":"226.74061435",
            "holds":"0"
        },
        {
            "frozen":"0",
            "hold":"0",
            "id": "",
            "currency":"EOS",
            "balance":"0.4925",
            "available":"0.4925",
            "holds":"0"
        }
        ]
        """
        success, error = await self._rest_api.get_account_balance()
        if error:
            state = State("get_account_balance error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return
        self._update_asset(success)
        
        # Fetch orders from server. (open + partially filled)
        for sym in self._symbols:
            """
            [
            {
                "client_oid":"oktspot86",
                "created_at":"2019-03-20T03:28:14.000Z",
                "filled_notional":"0",
                "filled_size":"0",
                "funds":"",
                "instrument_id":"BTC-USDT",
                "notional":"",
                "order_id":"2511109744100352",
                "order_type":"0",
                "price":"3594.7",
                "price_avg":"",
                "product_id":"BTC-USDT",
                "side":"buy",
                "size":"0.001",
                "status":"open",
                "state":"0",
                "timestamp":"2019-03-20T03:28:14.000Z",
                "type":"limit"
            }
            ]
            """
            success, error = await self._rest_api.get_open_orders(sym)
            if error:
                state = State("get open orders error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                SingleTask.run(self.cb.on_state_update_callback, state)
                return
            for order_info in success:
                self._update_order(order_info)

        # Subscribe order channel.
        data = {
            "op": "subscribe",
            "args": self._order_channel
        }
        await self.send_json(data)
        
        #订阅账户余额通知
        sl = []
        for si in self._syminfo:
            sl.append(si["base_currency"])
            sl.append(si["quote_currency"])
        #set的目的是去重
        self._account_channel = []
        for s in set(sl):
            self._account_channel.append("spot/account:{symbol}".format(symbol=s))
        #发送订阅
        data = {
            "op": "subscribe",
            "args": self._account_channel
        }
        await self.send_json(data)
        
        #计数初始化0
        self._subscribe_response_count = 0

    async def process_binary(self, raw):
        """ Process binary message that received from websocket.

        Args:
            raw: Binary message received from websocket.

        Returns:
            None.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        if msg == "pong":
            return
        logger.debug("msg:", msg, caller=self)
        msg = json.loads(msg)

        # Authorization message received.
        if msg.get("event") == "login":
            if not msg.get("success"):
                state = State("Websocket connection authorized failed: {}".format(msg), State.STATE_CODE_GENERAL_ERROR)
                logger.error(state, caller=self)
                SingleTask.run(self.cb.on_state_update_callback, state)
                return
            logger.info("Websocket connection authorized successfully.", caller=self)
            await self._auth_success_callback()

        # Subscribe response message received.
        elif msg.get("event") == "subscribe":
            #msg.get("channel")
            self._subscribe_response_count = self._subscribe_response_count + 1 #每来一次订阅响应计数就加一
            count = len(self._account_channel)+len(self._order_channel) #应该要返回的订阅响应数
            if self._subscribe_response_count == count: #所有的订阅都成功了,通知上层接口都准备好了
                state = State("Environment ready", State.STATE_CODE_READY)
                SingleTask.run(self.cb.on_state_update_callback, state)
        
        elif msg.get("event") == "error":
            state = State("Websocket processing failed: {}".format(msg), State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)

        # Order update message received.
        elif msg.get("table") == "spot/order":
            """
            {
            "table":"spot/order",
            "data":[
                {
                "client_oid":"",
                "filled_notional":"0",
                "filled_size":"0",
                "instrument_id":"ETC-USDT",
                "last_fill_px":"0",
                "last_fill_qty":"0",
                "last_fill_time":"1970-01-01T00:00:00.000Z",
                "margin_trading":"1",
                "notional":"",
                "order_id":"3576398568830976",
                "order_type":"0",
                "price":"5.826",
                "side":"buy",
                "size":"0.1",
                "state":"0",
                "status":"open",
                "timestamp":"2019-09-24T06:45:11.394Z",
                "type":"limit",
                "created_at":"2019-09-24T06:45:11.394Z"
                }
                ]
            }
            """
            for data in msg["data"]:
                self._update_order(data)

        elif msg.get("table") == "spot/account":
            self._update_asset(msg["data"])

    def _convert_asset_format(self, data):
        for d in data:
            c = d["currency"]
            self._assets[c]["free"] = float(d["available"])
            self._assets[c]["locked"] = float(d["hold"])
            self._assets[c]["total"] = float(d["balance"])
        return Asset(self._platform, self._account, self._assets, tools.get_cur_timestamp_ms(), True)
    
    def _update_asset(self, data):
        ast = self._convert_asset_format(data)
        if self.cb.on_asset_update_callback:
            SingleTask.run(self.cb.on_asset_update_callback, ast)

    def _convert_order_format(self, order_info):
        order_no = str(order_info["order_id"])
        symbol = order_info["instrument_id"]
        remain = float(order_info["size"]) - float(order_info["filled_size"])
        ctime = tools.utctime_str_to_mts(order_info["created_at"])
        utime = tools.utctime_str_to_mts(order_info["timestamp"])
        state = order_info["state"]
        if state == "-2":
            status = ORDER_STATUS_FAILED
        elif state == "-1":
            status = ORDER_STATUS_CANCELED
        elif state == "0":
            status = ORDER_STATUS_SUBMITTED
        elif state == "1":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "2":
            status = ORDER_STATUS_FILLED
        else:
            status = ORDER_STATUS_NONE
        if order_info["type"] == "market":
            order_type = ORDER_TYPE_MARKET
        else:
            if order_info["order_type"] == "3":
                order_type = ORDER_TYPE_IOC
            else:
                order_type = ORDER_TYPE_LIMIT
        info = {
            "platform": self._platform,
            "account": self._account,
            "strategy": self._strategy,
            "order_no": order_no,
            "action": ORDER_ACTION_BUY if order_info["side"] == "buy" else ORDER_ACTION_SELL,
            "symbol": symbol,
            "price": float(order_info["price"]),
            "quantity": float(order_info["size"]),
            "remain": remain,
            "order_type": order_type,
            "status": status,
            "ctime": ctime,
            "utime": utime
            #"avg_price": order_info["price_avg"] filled_notional/filled_size???
        }
        return Order(**info)

    def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.

        Returns:
            None.
        """
        if order_info["margin_trading"] != "1": # 1.币币交易订单 2.杠杆交易订单
            return
        order = self._convert_order_format(order_info)
        if self.cb.on_order_update_callback:
            SingleTask.run(self.cb.on_order_update_callback, order)
        #=====================================================================================
        #如果存在成交部分,就处理成交回调
        if (order.status == ORDER_STATUS_PARTIAL_FILLED or order.status == ORDER_STATUS_FILLED) and \
           order_info.get("last_fill_px") and order_info.get("last_fill_qty"):
            #liquidity = LIQUIDITY_TYPE_TAKER if order_info["role"]=="taker" else LIQUIDITY_TYPE_MAKER #原始数据中没有
            #fee = float(order_info["fees"]) #原始数据中没有,可以考虑自己计算
            f = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "fill_no": order_info["last_fill_id"],
                "order_no": str(order_info["order_id"]),
                "side": ORDER_ACTION_BUY if order_info["side"] == "buy" else ORDER_ACTION_SELL, #成交方向,买还是卖
                "symbol": order_info["instrument_id"],
                "price": float(order_info["last_fill_px"]), #成交价格
                "quantity": float(order_info["last_fill_qty"]), #成交数量
                #"liquidity": liquidity, #maker成交还是taker成交
                #"fee": fee,
                "ctime": tools.utctime_str_to_mts(order_info["last_fill_time"])
            }
            fill = Fill(**f)
            if self.cb.on_fill_update_callback:
                SingleTask.run(self.cb.on_fill_update_callback, fill)


class OKExMarket(Websocket):
    """ OKEx Market Server.
    """
    
    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._symbols = kwargs["symbols"]
        self._wss = "wss://real.okex.com:8443"
        url = self._wss + "/ws/v3"
        super(OKExMarket, self).__init__(url, send_hb_interval=5, **kwargs)
        self.heartbeat_msg = "ping"
        
        self._orderbook_length = 20
        self._orderbooks = {}  # 订单薄数据 {"symbol": {"bids": {"price": quantity, ...}, "asks": {...}}}

    async def connected_callback(self):
        """After create Websocket connection successfully, we will subscribing orderbook/trade/kline."""
        ches = []
        for ch in ["orderbook", "trade", "kline", "ticker"]:
            if ch == "orderbook" and self.cb.on_orderbook_update_callback:
                for symbol in self._symbols:
                    ch = "spot/depth:{s}".format(s=symbol)
                    ches.append(ch)
            elif ch == "trade" and self.cb.on_trade_update_callback:
                for symbol in self._symbols:
                    ch = "spot/trade:{s}".format(s=symbol)
                    ches.append(ch)
            elif ch == "kline" and self.cb.on_kline_update_callback:
                for symbol in self._symbols:
                    ch = "spot/candle60s:{s}".format(s=symbol)
                    ches.append(ch)
            elif ch == "ticker" and self.cb.on_ticker_update_callback:
                for symbol in self._symbols:
                    ch = "spot/ticker:{s}".format(s=symbol)
                    ches.append(ch)
        if ches:
            msg = {
                "op": "subscribe",
                "args": ches
            }
            await self.send_json(msg)

    async def process_binary(self, raw):
        """ Process binary message that received from Websocket connection.

        Args:
            raw: Raw message that received from Websocket connection.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        # logger.debug("msg:", msg, caller=self)
        if msg == "pong":
            return
        msg = json.loads(msg)
        table = msg.get("table")
        if table == "spot/depth":
            if msg.get("action") == "partial":
                for d in msg["data"]:
                    await self.process_orderbook_partial(d)
            elif msg.get("action") == "update":
                for d in msg["data"]:
                    await self.process_orderbook_update(d)
            else:
                logger.warn("unhandle msg:", msg, caller=self)
        elif table == "spot/trade":
            for d in msg["data"]:
                await self.process_trade(d)
        elif table == "spot/candle60s":
            for d in msg["data"]:
                await self.process_kline(d)
        elif table == "spot/ticker":
            for d in msg["data"]:
                await self.process_ticker(d)

    async def process_orderbook_partial(self, data):
        """Process orderbook partical data."""
        symbol = data.get("instrument_id")
        if symbol not in self._symbols:
            return
        asks = data.get("asks")
        bids = data.get("bids")
        self._orderbooks[symbol] = {"asks": {}, "bids": {}, "timestamp": 0}
        for ask in asks:
            price = float(ask[0])
            quantity = float(ask[1])
            self._orderbooks[symbol]["asks"][price] = quantity
        for bid in bids:
            price = float(bid[0])
            quantity = float(bid[1])
            self._orderbooks[symbol]["bids"][price] = quantity
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))
        self._orderbooks[symbol]["timestamp"] = timestamp

    async def process_orderbook_update(self, data):
        """Process orderbook update data."""
        symbol = data.get("instrument_id")
        asks = data.get("asks")
        bids = data.get("bids")
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))

        if symbol not in self._orderbooks:
            return
        self._orderbooks[symbol]["timestamp"] = timestamp

        for ask in asks:
            price = float(ask[0])
            quantity = float(ask[1])
            if quantity == 0 and price in self._orderbooks[symbol]["asks"]:
                self._orderbooks[symbol]["asks"].pop(price)
            else:
                self._orderbooks[symbol]["asks"][price] = quantity

        for bid in bids:
            price = float(bid[0])
            quantity = float(bid[1])
            if quantity == 0 and price in self._orderbooks[symbol]["bids"]:
                self._orderbooks[symbol]["bids"].pop(price)
            else:
                self._orderbooks[symbol]["bids"][price] = quantity

        await self.publish_orderbook(symbol)

    async def publish_orderbook(self, symbol):
        ob = self._orderbooks[symbol]
        if not ob["asks"] or not ob["bids"]:
            logger.warn("symbol:", symbol, "asks:", ob["asks"], "bids:", ob["bids"], caller=self)
            return

        ask_keys = sorted(list(ob["asks"].keys()))
        bid_keys = sorted(list(ob["bids"].keys()), reverse=True)
        if ask_keys[0] <= bid_keys[0]:
            logger.warn("symbol:", symbol, "ask1:", ask_keys[0], "bid1:", bid_keys[0], caller=self)
            return

        asks = []
        for k in ask_keys[:self._orderbook_length]:
            price = k
            quantity = ob["asks"].get(k)
            asks.append([price, quantity])

        bids = []
        for k in bid_keys[:self._orderbook_length]:
            price = k
            quantity = ob["bids"].get(k)
            bids.append([price, quantity])

        data = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
            "timestamp": ob["timestamp"]
        }
        ob = Orderbook(**data)
        SingleTask.run(self.cb.on_orderbook_update_callback, ob)

    async def process_trade(self, data):
        symbol = data.get("instrument_id")
        if symbol not in self._symbols:
            return
        action = ORDER_ACTION_BUY if data["side"] == "buy" else ORDER_ACTION_SELL
        price = float(data["price"])
        quantity = float(data["size"])
        timestamp = tools.utctime_str_to_mts(data["timestamp"])

        data = {
            "platform": self._platform,
            "symbol": symbol,
            "action": action,
            "price": price,
            "quantity": quantity,
            "timestamp": timestamp
        }
        trade = Trade(**data)
        SingleTask.run(self.cb.on_trade_update_callback, trade)

    async def process_kline(self, data):
        symbol = data["instrument_id"]
        if symbol not in self._symbols:
            return
        timestamp = tools.utctime_str_to_mts(data["candle"][0])
        _open = float(data["candle"][1])
        high = float(data["candle"][2])
        low = float(data["candle"][3])
        close = float(data["candle"][4])
        volume = float(data["candle"][5])
        data = {
            "platform": self._platform,
            "symbol": symbol,
            "open": _open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "timestamp": timestamp,
            "kline_type": MARKET_TYPE_KLINE
        }
        kline = Kline(**data)
        SingleTask.run(self.cb.on_kline_update_callback, kline)

    async def process_ticker(self, data):
        """
        {
        "table":"spot/ticker",
        "data":[
            {
                "instrument_id":"ETH-USDT",
                "last":"146.24",
                "last_qty":"0.082483",
                "best_bid":"146.24",
                "best_bid_size":"0.006822",
                "best_ask":"146.25",
                "best_ask_size":"80.541709",
                "open_24h":"147.17",
                "high_24h":"147.48",
                "low_24h":"143.88",
                "base_volume_24h":"117387.58",
                "quote_volume_24h":"17159427.21",
                "timestamp":"2019-12-11T02:31:40.436Z"
            }
        ]
        }
        """
        p = {
            "platform": self._platform,
            "symbol": data["instrument_id"],
            "ask": float(data["best_ask"]),
            "bid": float(data["best_bid"]),
            "last": float(data["last"]),
            "timestamp": tools.utctime_str_to_mts(data["timestamp"])
        }
        ticker = Ticker(**p)
        SingleTask.run(self.cb.on_ticker_update_callback, ticker)