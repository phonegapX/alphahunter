# -*- coding:utf-8 -*-

"""
Huobi Future Trade module.
https://www.hbdm.com/zh-cn/contract/exchange/

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import gzip
import json
import copy
import hmac
import base64
import urllib
import hashlib
import datetime
from urllib.parse import urljoin
from collections import defaultdict, deque
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional, Any

from quant.gateway import ExchangeGateway
from quant.state import State
from quant.order import Order, Fill
from quant.utils import tools
from quant.utils import logger
from quant.tasks import SingleTask
from quant.position import Position
from quant.const import MARKET_TYPE_KLINE, INDICATE_ORDER, INDICATE_ASSET, INDICATE_POSITION
from quant.asset import Asset
from quant.utils.websocket import Websocket
from quant.utils.http_client import AsyncHttpRequests
from quant.utils.decorator import async_method_locker
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED, ORDER_STATUS_NONE
from quant.order import TRADE_TYPE_BUY_OPEN, TRADE_TYPE_SELL_OPEN, TRADE_TYPE_BUY_CLOSE, TRADE_TYPE_SELL_CLOSE
from quant.order import LIQUIDITY_TYPE_MAKER, LIQUIDITY_TYPE_TAKER
from quant.market import Kline, Orderbook, Trade, Ticker


__all__ = ("HuobiFutureRestAPI", "HuobiFutureTrader", )


class HuobiFutureRestAPI:
    """ OKEx Swap REST API client.

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY Passphrase.
    """

    def __init__(self, host, access_key, secret_key):
        """initialize REST API client."""
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key

    async def get_contract_info(self, symbol=None, contract_type=None, contract_code=None):
        """ Get contract information.

        Args:
            symbol: Trade pair, default `None` will return all symbols.
            contract_type: Contract type, `this_week` / `next_week` / `quarter`, default `None` will return all types.
            contract_code: Contract code, e.g. BTC180914.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/api/v1/contract_contract_info"
        params = {}
        if symbol:
            params["symbol"] = symbol
        if contract_type:
            params["contract_type"] = contract_type
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params)
        return success, error

    async def get_price_limit(self, symbol=None, contract_type=None, contract_code=None):
        """ Get contract price limit.

        Args:
            symbol: Trade pair, default `None` will return all symbols.
            contract_type: Contract type, `this_week` / `next_week` / `quarter`, default `None` will return all types.
            contract_code: Contract code, e.g. BTC180914.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/api/v1/contract_price_limit"
        params = {}
        if symbol:
            params["symbol"] = symbol
        if contract_type:
            params["contract_type"] = contract_type
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_orderbook(self, symbol):
        """ Get orderbook information.

        Args:
            symbol: Symbol name, `BTC_CW` - current week, `BTC_NW` next week, `BTC_CQ` current quarter.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/market/depth"
        params = {
            "symbol": symbol,
            "type": "step0"
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_asset_info(self):
        """ Get account asset information.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_account_info"
        success, error = await self.request("POST", uri, auth=True)
        return success, error

    async def get_position(self, symbol=None):
        """ Get position information.

        Args:
            symbol: Currency name, e.g. BTC. default `None` will return all types.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_position_info"
        body = {}
        if symbol:
            body["symbol"] = symbol
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_order(self, symbol, contract_type, contract_code, price, quantity, direction, offset, lever_rate, order_price_type):
        """ Create an new order.

        Args:
            symbol: Currency name, e.g. BTC.
            contract_type: Contract type, `this_week` / `next_week` / `quarter`.
            contract_code: Contract code, e.g. BTC180914.
            price: Order price.
            quantity: Order amount.
            direction: Transaction direction, `buy` / `sell`.
            offset: `open` / `close`.
            lever_rate: Leverage rate, 10 or 20.
            order_price_type: Order type, `limit` - limit order, `opponent` - market order.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_order"
        body = {
            "symbol": symbol,
            "contract_type": contract_type,
            "contract_code": contract_code,
            "price": price,
            "volume": quantity,
            "direction": direction,
            "offset": offset,
            "lever_rate": lever_rate,
            "order_price_type": order_price_type
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_orders(self, symbol, order_ids):
        """ Revoke one or multiple orders.

        Args:
            symbol: Currency name, e.g. BTC.
            order_ids: Order ID list.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_cancel"
        body = {
            "symbol": symbol,
            "order_id": ",".join(order_ids)
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order_all(self, symbol, contract_type=None, contract_code=None):
        """ Revoke all orders.

        Args:
            symbol: Currency name, e.g. BTC.
            contract_type: Contract type, `this_week` / `next_week` / `quarter`, default `None` will return all types.
            contract_code: Contract code, e.g. BTC180914.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/api/v1/contract_cancelall"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        if contract_type:
            body["symbol"] = symbol
            body["contract_type"] = contract_type
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_order_info(self, symbol, order_ids):
        """ Get order information.

        Args:
            symbol: Currency name, e.g. BTC.
            order_ids: Order ID list. (different IDs are separated by ",", maximum 20 orders can be withdrew at one time.)

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_order_info"
        body = {
            "symbol": symbol,
            "order_id": ",".join(order_ids)
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_open_orders(self, symbol, index=1, size=50):
        """ Get open order information.

        Args:
            symbol: Currency name, e.g. BTC.
            index: Page index, default 1st page.
            size: Page size, Default 20，no more than 50.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_openorders"
        body = {
            "symbol": symbol,
            "page_index": index,
            "page_size": size
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. `GET` / `POST` / `DELETE` / `PUT`.
            uri: HTTP request uri.
            params: HTTP query params.
            body: HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        url = urljoin(self._host, uri)

        if auth:
            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            params = params if params else {}
            params.update({"AccessKeyId": self._access_key,
                           "SignatureMethod": "HmacSHA256",
                           "SignatureVersion": "2",
                           "Timestamp": timestamp})
            params["Signature"] = self.generate_signature(method, params, uri)

        if not headers:
            headers = {}
        if method == "GET":
            headers["Content-type"] = "application/x-www-form-urlencoded"
            headers["User-Agent"] = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36"
        else:
            headers["Accept"] = "application/json"
            headers["Content-type"] = "application/json"
            headers["User-Agent"] = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0"
        _, success, error = await AsyncHttpRequests.fetch(method, url, params=params, data=body, headers=headers, timeout=10)
        if error:
            return None, error
        if not isinstance(success, dict):
            result = json.loads(success)
        else:
            result = success
        if result.get("status") != "ok":
            return None, result
        return result, None

    def generate_signature(self, method, params, request_path):
        host_url = urllib.parse.urlparse(self._host).hostname.lower()
        sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.parse.urlencode(sorted_params)
        payload = [method, host_url, request_path, encode_params]
        payload = "\n".join(payload)
        payload = payload.encode(encoding="UTF8")
        secret_key = self._secret_key.encode(encoding="utf8")
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature


class HuobiFutureTrader(Websocket, ExchangeGateway):
    """ Huobi Future Trade module.
    """

    def __init__(self, **kwargs):
        """Initialize."""
        self.cb = kwargs["cb"]
        state = None
        if kwargs.get("account") and (not kwargs.get("access_key") or not kwargs.get("secret_key")):
            state = State("param access_key or secret_key miss")
        elif not kwargs.get("strategy"):
            state = State("param strategy miss")
        elif not kwargs.get("symbols"):
            state = State("param symbols miss")
        elif not kwargs.get("platform"):
            state = State("param platform miss")
            
        if state:
            logger.error(state, caller=self)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return

        self._account = kwargs.get("account")
        self._access_key = kwargs.get("access_key")
        self._secret_key = kwargs.get("secret_key")
        self._strategy = kwargs["strategy"]
        self._platform = kwargs["platform"]
        self._symbols = kwargs["symbols"]
        self._host = "https://api.btcgateway.pro"
        self._wss = "wss://api.btcgateway.pro"
        
        url = self._wss + "/notification"
        super(HuobiFutureTrader, self).__init__(url, send_hb_interval=0, **kwargs)
        #self.heartbeat_msg = "ping"

        # Initializing our REST API client.
        self._rest_api = HuobiFutureRestAPI(self._host, self._access_key, self._secret_key)
        
        self._syminfo:DefaultDict[str: Dict[str, Any]] = defaultdict(dict)

        #e.g. {"BTC": {"free": 1.1, "locked": 2.2, "total": 3.3}, ... }
        self._assets: DefaultDict[str: Dict[str, float]] = defaultdict(lambda: {k: 0.0 for k in {'free', 'locked', 'total'}})

        self._fills: DefaultDict[str, DefaultDict[str, DefaultDict[str, Fill]]] = defaultdict(lambda:defaultdict(lambda:defaultdict(None))) #三级字典

        if self._account != None:
            self.initialize()

        #如果四个行情回调函数都为空的话,就根本不需要执行市场行情相关代码
        if (self.cb.on_kline_update_callback or 
            self.cb.on_orderbook_update_callback or 
            self.cb.on_trade_update_callback or 
            self.cb.on_ticker_update_callback):
            #市场行情数据
            HuobiFutureMarket(**kwargs)

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, *args, **kwargs):
        """ Create an order.

        Args:
            symbol: Contract code
            action: Trade direction, BUY or SELL.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, LIMIT or MARKET.
            kwargs:
                lever_rate: Leverage rate, 10 or 20.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if int(quantity) > 0:
            if action == ORDER_ACTION_BUY: #买入开多
                direction = "buy"
                offset = "open"
            elif action == ORDER_ACTION_SELL: #卖出平多
                direction = "sell"
                offset = "close"
            else:
                return None, "action error"
        elif int(quantity) < 0:
            if action == ORDER_ACTION_BUY: #买入平空
                direction = "buy"
                offset = "close"
            elif action == ORDER_ACTION_SELL: #卖出开空
                direction = "sell"
                offset = "open"
            else:
                return None, "action error"
        else:
            return None, "quantity error"

        lever_rate = kwargs.get("lever_rate", 10)
        
        if order_type == ORDER_TYPE_LIMIT:
            order_price_type = "limit"
        elif order_type == ORDER_TYPE_MARKET:
            order_price_type = "opponent"
        else:
            return None, "order type error"

        quantity = abs(int(quantity))
        result, error = await self._rest_api.create_order(None, None, symbol, price, quantity, direction, offset, lever_rate, order_price_type)
        if error:
            return None, error
        if result["status"] != "ok":
            return None, result
        return str(result["data"]["order_id"]), None

    async def revoke_order(self, symbol, *order_nos):
        """ Revoke (an) order(s).

        Args:
            symbol: Contract code
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel
                all orders for this symbol(initialized in Trade object). If you set 1 param, you can cancel an order.
                If you set multiple param, you can cancel multiple orders. Do not set param length more than 100.

        Returns:
            Success or error, see bellow.
        """
        """
        {
        "status": "ok",
        "data": {
          "errors":[
            {
              "order_id":"633766664829804544",
              "err_code": 200417,
              "err_msg": "invalid symbol"
            },
            {
              "order_id":"633766664829804544",
              "err_code": 200415,
              "err_msg": "invalid symbol"
             }
          ],
          "successes":"161256,1344567"
         },
        "ts": 1490759594752
        }
        """
        if len(order_nos) == 0: #删除指定符号下所有订单
            s, e = await self._rest_api.revoke_order_all(None, None, symbol)
        else: #删除指定符号下单个或者多个订单
            info = self._syminfo[symbol]
            if not info:
                return None, "Symbol not exist"
            s, e = await self._rest_api.revoke_orders(info["symbol"], order_nos)
        if len(order_nos) == 1: #删除一个订单情况下的返回格式
            if e:
                return order_nos[0], e
            if s["status"] != "ok":
                return order_nos[0], s
            if s["errors"]:
                return order_nos[0], s["errors"][0]
            return order_nos[0], None
        elif len(order_nos) == 0 or len(order_nos) > 1: #删除多个或者全部订单情况下的返回格式
            if e:
                return [], e
            if s["status"] != "ok":
                return [], s
            result = []
            if s["successes"]:
                success = s["successes"]
                for x in success.split(","):
                    result.append((x, None))
            for x in s["errors"]:
                result.append((x["order_id"], x["err_msg"]))
            return result, None

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Contract code

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        """
        {
        "status": "ok",
        "data":{
          "orders":[
            {
               "symbol": "BTC",
               "contract_type": "this_week",
               "contract_code": "BTC180914",
               "volume": 111,
               "price": 1111,
               "order_price_type": "limit",
               "order_type": 1,
               "direction": "buy",
               "offset": "open",
               "lever_rate": 10,
               "order_id": 633766664829804544,
               "order_id_str": "633766664829804544",
               "client_order_id": 10683,
               "order_source": "web",
               "created_at": 1408076414000,
               "trade_volume": 1,
               "trade_turnover": 1200,
               "fee": 0,
               "trade_avg_price": 10,
               "margin_frozen": 10,
               "profit": 0,
               "status": 1,
               "fee_asset": "BTC"
              }
             ],
          "total_page":15,
          "current_page":3,
          "total_size":3
         },
        "ts": 1490759594752
        }
        """
        info = self._syminfo[symbol]
        if not info:
            return None, "Symbol not exist"
        s = info["symbol"]
        success, error = await self._rest_api.get_open_orders(s)
        if error:
            return None, error
        if success["status"] != "ok":
            return None, success
        orders = []
        for order_info in success["data"]["orders"]:
            if order_info["contract_code"] == symbol:
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
        success, error = await self._rest_api.get_asset_info()
        if error:
            return None, error
        if success["status"] != "ok":
            return None, success
        ast = self._convert_asset_format(success)
        return ast

    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Contract code

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        """
        {
        "status": "ok",
        "data": [
          {
            "symbol": "BTC",
            "contract_code": "BTC180914",
            "contract_type": "this_week",
            "volume": 1,
            "available": 0,
            "frozen": 0.3,
            "cost_open": 422.78,
            "cost_hold": 422.78,
            "profit_unreal": 0.00007096,
            "profit_rate": 0.07,
            "profit": 0.97,
            "position_margin": 3.4,
            "lever_rate": 10,
            "direction":"buy",
            "last_price":7900.17
           }
          ],
          "ts": 158797866555
        }
        """
        info = self._syminfo[symbol]
        if not info:
            return None, "Symbol not exist"
        s = info["symbol"]
        success, error = await self._rest_api.get_position(s)
        if error:
            return None, error
        if success["status"] != "ok":
            return None, success
        utime = success["ts"]
        for position_info in success["data"]:
            if position_info["contract_code"] == symbol:
                pos = self._convert_position_format(position_info, utime)
                return pos, None
        return None, "Position not exist"

    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Contract code

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        info = self._syminfo[symbol]
        if not info:
            return None, "Symbol not exist"
        price_tick = info["price_tick"]
        size_tick = 1
        size_limit = 1
        value_tick = None
        value_limit = None
        base_currency = None
        quote_currency = None
        syminfo = SymbolInfo(self._platform, symbol, price_tick, size_tick, size_limit, value_tick, value_limit, base_currency, quote_currency)
        return syminfo, None

    async def invalid_indicate(self, symbol, indicate_type):
        """ update (an) callback function.

        Args:
            symbol: Contract code
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
            elif indicate_type == INDICATE_POSITION and self.cb.on_position_update_callback:
                success, error = await self.get_position(symbol)
                if error:
                    state = State("get_position error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                    SingleTask.run(self.cb.on_state_update_callback, state)
                    return
                SingleTask.run(self.cb.on_position_update_callback, success)
        if indicate_type == INDICATE_ORDER or indicate_type == INDICATE_ASSET or indicate_type == INDICATE_POSITION:
            SingleTask.run(_task)
            return True, None
        else:
            logger.error("indicate_type error! indicate_type:", indicate_type, caller=self)
            return False, "indicate_type error"    

    @property
    def rest_api(self):
        return self._rest_api

    async def connected_callback(self):
        """After connect to Websocket server successfully, send a auth message to server."""
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        data = {
            "AccessKeyId": self._access_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": timestamp
        }
        sign = self._rest_api.generate_signature("GET", data, "/notification")
        data["op"] = "auth"
        data["type"] = "api"
        data["Signature"] = sign
        await self.send_json(data)

    async def _init_symbol_info(self):
        """
        {
            "status": "ok",
            "data": [
              {
                "symbol": "BTC",
                "contract_code": "BTC180914",
                "contract_type": "this_week",
                "contract_size": 100,
                "price_tick": 0.001,
                "delivery_date": "20180704",
                "create_date": "20180604",
                "contract_status": 1
               }
              ],
            "ts":158797866555
        }
        """
        success, error = await self._rest_api.get_contract_info()
        if error:
            return False, error
        for info in success["data"]:
            #如"BTC_CW"表示BTC当周合约，"BTC_NW"表示BTC次周合约，"BTC_CQ"表示BTC季度合约
            t = info["contract_type"]
            if t == "this_week":
                info["symbol_raw"] = info["symbol"] + "_CW"
            elif t == "next_week":
                info["symbol_raw"] = info["symbol"] + "_NW"
            elif t == "quarter":
                info["symbol_raw"] = info["symbol"] + "_CQ"
            self._syminfo[info["contract_code"]] = info #符号信息一般不变,获取一次保存好,其他地方要用直接从这个变量获取就可以了
        #返回成功
        return True, None

    async def _init_sub_channel(self):
        self._order_channel = []
        self._position_channel = []
        self._asset_channel = []
        for sym in self._symbols:
            info = self._syminfo[sym]
            if info:
                s = info["symbol"].lower()
                self._order_channel.append("orders.{}".format(s))
                self._position_channel.append("positions.{}".format(s))
                self._asset_channel.append("accounts.{}".format(s))
        #去重
        self._order_channel = list(set(self._order_channel))
        self._position_channel = list(set(self._position_channel))
        self._asset_channel = list(set(self._asset_channel))
        #
        self._subscribe_response_count = 0
        self._subscribe_channel_total = len(self._order_channel) + len(self._position_channel) + len(self._asset_channel)

    async def auth_callback(self, data):
        if data["err-code"] != 0:
            state = State("Websocket connection authorized failed: {}".format(data), State.STATE_CODE_GENERAL_ERROR)
            logger.error(state, caller=self)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return

        success, error = await self._init_symbol_info()
        if error:
            state = State("_init_symbol_info error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)
            #初始化过程中发生错误,关闭网络连接,触发重连机制
            await self.socket_close()
            return

        #获取当前未完成订单
        if self.cb.on_order_update_callback:
            for sym in self._symbols:
                success, error = await self.get_orders(sym)
                if error:
                    state = State("get_orders error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                    SingleTask.run(self.cb.on_state_update_callback, state)
                    #初始化过程中发生错误,关闭网络连接,触发重连机制
                    await self.socket_close()
                    return
                for order in success:
                    SingleTask.run(self.cb.on_order_update_callback, order)

        #获取当前持仓
        if self.cb.on_position_update_callback:
            for sym in self._symbols:
                success, error = await self.get_position(sym)
                if error:
                    state = State("get_position error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                    SingleTask.run(self.cb.on_state_update_callback, state)
                    #初始化过程中发生错误,关闭网络连接,触发重连机制
                    await self.socket_close()
                    return
                SingleTask.run(self.cb.on_position_update_callback, success)

        #获取当前账户余额
        if self.cb.on_asset_update_callback:
            success, error = await self.get_assets()
            if error:
                state = State("get_assets error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                SingleTask.run(self.cb.on_state_update_callback, state)
                #初始化过程中发生错误,关闭网络连接,触发重连机制
                await self.socket_close()
                return
            SingleTask.run(self.cb.on_asset_update_callback, success)

        await self._init_sub_channel()

        # subscribe order
        if self.cb.on_order_update_callback or self.cb.on_fill_update_callback:
            for ch in self._order_channel:
                params = {
                    "op": "sub",
                    "cid": tools.get_uuid1(),
                    "topic": ch
                }
                await self.send_json(params)

        # subscribe position
        if self.cb.on_position_update_callback:
            for ch in self._position_channel:
                params = {
                    "op": "sub",
                    "cid": tools.get_uuid1(),
                    "topic": ch
                }
                await self.send_json(params)

        # subscribe asset
        if self.cb.on_asset_update_callback:
            for ch in self._asset_channel:
                params = {
                    "op": "sub",
                    "cid": tools.get_uuid1(),
                    "topic": ch
                }
                await self.send_json(params)

    async def sub_callback(self, data):
        if data["err-code"] != 0:
            state = State("subscribe {} failed!".format(data["topic"]), State.STATE_CODE_GENERAL_ERROR)
            logger.error(state, caller=self)
            SingleTask.run(self.cb.on_state_update_callback, state)
        else:
            self._subscribe_response_count = self._subscribe_response_count + 1 #每来一次订阅响应计数就加一
            if self._subscribe_response_count == self._subscribe_channel_total: #所有的订阅都成功了,通知上层接口都准备好了
                state = State("Environment ready", State.STATE_CODE_READY)
                SingleTask.run(self.cb.on_state_update_callback, state)

    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
        """
        data = json.loads(gzip.decompress(raw).decode())
        logger.debug("data:", data, caller=self)

        op = data.get("op")
        if op == "ping":
            hb_msg = {"op": "pong", "ts": data.get("ts")}
            await self.send_json(hb_msg)

        elif op == "auth":
            await self.auth_callback(data)

        elif op == "sub":
            await self.sub_callback(data)

        elif op == "notify":
            if data["topic"] in self._order_channel:
                self._update_order(data)
            elif data["topic"] == "positions" or data["topic"] in self._position_channel:
                self._update_position(data)
            elif data["topic"] == "accounts" or data["topic"] in self._asset_channel:
                self._update_asset(data)

    def _convert_order_format(self, order_info):
        symbol = order_info["contract_code"]
        if symbol not in self._symbols:
            return
        order_no = str(order_info["order_id"])
        if order_info["direction"] == "buy":
            if order_info["offset"] == "open":
                trade_type = TRADE_TYPE_BUY_OPEN #买入开多
            else:
                trade_type = TRADE_TYPE_BUY_CLOSE #买入平空
        else:
            if order_info["offset"] == "close":
                trade_type = TRADE_TYPE_SELL_CLOSE #卖出平多
            else:
                trade_type = TRADE_TYPE_SELL_OPEN #卖出开空
        quantity = order_info["volume"]
        state = order_info["status"]
        #(1准备提交 2准备提交 3已提交 4部分成交 5部分成交已撤单 6全部成交 7已撤单 11撤单中)
        if state in [1, 2, 3]:
            status = ORDER_STATUS_SUBMITTED
        elif state == 4:
            status = ORDER_STATUS_PARTIAL_FILLED
            remain = int(quantity) - int(order_info["trade_volume"])
        elif state == 6:
            status = ORDER_STATUS_FILLED
            remain = 0
        elif state in [5, 7]:
            status = ORDER_STATUS_CANCELED
            remain = int(quantity) - int(order_info["trade_volume"])
        else:
            status = ORDER_STATUS_NONE
        #订单报价类型 "limit":限价 "opponent":对手价 "post_only":只做maker单,post only下单只受用户持仓数量限制
        info = {
            "platform": self._platform,
            "account": self._account,
            "strategy": self._strategy,
            "order_no": order_no,
            "action": ORDER_ACTION_BUY if order_info["direction"] == "buy" else ORDER_ACTION_SELL,
            "symbol": symbol,
            "price": order_info["price"],
            "quantity": quantity,
            "status": status,
            "order_type": ORDER_TYPE_LIMIT if order_info["order_price_type"] in ["limit","post_only"] else ORDER_TYPE_MARKET,
            "trade_type": trade_type,
            "avg_price": order_info["trade_avg_price"],
            "ctime": order_info["created_at"],
            "utime": order_info["ts"]
        }
        return Order(**info)

    def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.
        """
        """
        {
        "op": "notify",
        "topic": "orders.btc",
        "ts": 1489474082831,
        "symbol": "BTC",
        "contract_type": "this_week",
        "contract_code": "BTC180914",
        "volume": 111,
        "price": 1111,
        "order_price_type": "limit",
        "direction": "buy",
        "offset": "open",
        "status": 6,
        "lever_rate": 10,
        "order_id": 633989207806582784,
        "order_id_str": "633989207806582784",
        "client_order_id": 10683,
        "order_source": "web",
        "order_type": 1,
        "created_at": 1408076414000,
        "trade_volume": 1,
        "trade_turnover": 1200,
        "fee": 0,
        "trade_avg_price": 10,
        "margin_frozen": 10,
        "profit": 2,
        "trade": [{
            "id": "2131234825-6124591349-1",
            "trade_id": 112,
            "trade_volume": 1,
            "trade_price": 123.4555,
            "trade_fee": 0.234,
            "trade_turnover": 34.123,
            "created_at": 1490759594752,
            "role": "maker"
        }]
        }
        """
        symbol = order_info["contract_code"]
        if symbol not in self._symbols:
            return
        order = self._convert_order_format(order_info)
        if self.cb.on_order_update_callback:
            SingleTask.run(self.cb.on_order_update_callback, order)
        self._update_fill(order_info)
        # Delete fills of order that already completed.
        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._fills[symbol].pop(order.order_no)

    def _update_fill(self, order_info):
        symbol = order_info["contract_code"]
        order_no = str(order_info["order_id"])
        for t in order_info["trade"]:
            fill_no = t["id"]
            if not self._fills[symbol][order_no][fill_no]: #保证不会重复通知上层
                price = t["trade_price"] #成交价格
                size = t["trade_volume"] #成交数量
                side = ORDER_ACTION_BUY if order_info["direction"] == "buy" else ORDER_ACTION_SELL   
                liquidity = LIQUIDITY_TYPE_TAKER if order_info["role"]=="taker" else LIQUIDITY_TYPE_MAKER
                fee = t["trade_fee"]
                f = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "fill_no": fill_no,
                    "order_no": order_no,
                    "side": side, #成交方向,买还是卖
                    "symbol": symbol,
                    "price": price, #成交价格
                    "quantity": size, #成交数量
                    "liquidity": liquidity, #maker成交还是taker成交
                    "fee": fee,
                    "ctime": t["created_at"]
                }
                fill = Fill(**f)
                self._fills[symbol][order_no][fill_no] = fill
                if self.cb.on_fill_update_callback:
                    SingleTask.run(self.cb.on_fill_update_callback, fill)

    def _convert_position_format(self, position_info, utime):
        symbol = position_info["contract_code"]
        pos = Position(self._platform, self._account, self._strategy, symbol)
        if position_info["direction"] == "buy":
            pos.long_quantity = int(position_info["volume"])
            pos.long_avg_price = position_info["cost_hold"]
        else:
            pos.short_quantity = int(position_info["volume"])
            pos.short_avg_price = position_info["cost_hold"]
        #pos.liquid_price = None
        pos.utime = utime
        return pos

    def _update_position(self, data):
        """ Position update.

        Args:
            data: Position information.

        Returns:
            None.
        """
        """
        {
        "op": "notify",
        "topic": "positions",
        "ts": 1489474082831,
        "event": "order.match",
        "data": [{
            "symbol": "BTC",
            "contract_code": "BTC180914",
            "contract_type": "this_week",
            "volume": 1,
            "available": 0,
            "frozen": 1,
            "cost_open": 422.78,
            "cost_hold": 422.78,
            "profit_unreal": 0.00007096,
            "profit_rate": 0.07,
            "profit": 0.97,
            "position_margin": 3.4,
            "lever_rate": 10,
            "direction": "sell",
            "last_price": 9584.41
        }]
        }
        """
        utime = data["ts"]
        for position_info in data["data"]:
            symbol = position_info["contract_code"]
            if symbol in self._symbols:
                pos = self._convert_position_format(position_info, utime)
                SingleTask.run(self.cb.on_position_update_callback, pos)

    def _convert_asset_format(self, data):
        for d in data["data"]:
            symbol = d["symbol"]
            total = d["margin_balance"]
            free = d["margin_available"]
            locked = d["margin_position"] + d["margin_frozen"]
            self._assets[symbol] = {
                "total": total,
                "free": free,
                "locked": locked
            }
        timestamp = data["ts"]
        ast = Asset(self._platform, self._account, self._assets, timestamp, True)
        return ast

    def _update_asset(self, data):
        """ asset update.

        Args:
            data: asset information.

        Returns:
            None.
        """
        """
        {
        "op": "notify",
        "topic": "accounts",
        "ts": 1489474082831,
        "event": "order.match",
        "data": [{
            "symbol": "BTC",
            "margin_balance": 1,
            "margin_static": 1,
            "margin_position": 0,
            "margin_frozen": 3.33,
            "margin_available": 0.34,
            "profit_real": 3.45,
            "profit_unreal": 7.45,
            "withdraw_available": 4.0989898,
            "risk_rate": 100,
            "liquidation_price": 100,
            "lever_rate": 10,
            "adjust_factor": 0.1
        }]
        }
        """
        ast = self._convert_asset_format(data)
        SingleTask.run(self.cb.on_asset_update_callback, ast)


class HuobiFutureMarket(Websocket):
    """ Huobi Future Market Server.
    """

    def __init__(self, **kwargs):
        self.cb = kwargs["cb"]
        self._platform = kwargs["platform"]
        self._symbols = kwargs["symbols"]
        self._host = "https://api.btcgateway.pro"
        self._wss = "wss://www.btcgateway.pro"
        url = self._wss + "/ws"
        super(HuobiFutureMarket, self).__init__(url, send_hb_interval=0, **kwargs)
        #self.heartbeat_msg = "ping"
        # Initializing our REST API client.
        self._rest_api = HuobiFutureRestAPI(self._host, None, None)
        self._c_to_s = {}  # {"channel": "symbol_raw"}
        self._s_to_cd = {}  # {"symbol_raw": "Contract code"}
        self._orderbook_length = 20
        self._syminfo:DefaultDict[str: Dict[str, Any]] = defaultdict(dict)
        self.initialize()

    async def _init_symbol_info(self):
        """
        {
            "status": "ok",
            "data": [
              {
                "symbol": "BTC",
                "contract_code": "BTC180914",
                "contract_type": "this_week",
                "contract_size": 100,
                "price_tick": 0.001,
                "delivery_date": "20180704",
                "create_date": "20180604",
                "contract_status": 1
               }
              ],
            "ts":158797866555
        }
        """
        success, error = await self._rest_api.get_contract_info()
        if error:
            return False, error
        for info in success["data"]:
            #如"BTC_CW"表示BTC当周合约，"BTC_NW"表示BTC次周合约，"BTC_CQ"表示BTC季度合约
            t = info["contract_type"]
            if t == "this_week":
                info["symbol_raw"] = info["symbol"] + "_CW"
            elif t == "next_week":
                info["symbol_raw"] = info["symbol"] + "_NW"
            elif t == "quarter":
                info["symbol_raw"] = info["symbol"] + "_CQ"
            self._syminfo[info["contract_code"]] = info #符号信息一般不变,获取一次保存好,其他地方要用直接从这个变量获取就可以了
        #返回成功
        return True, None

    async def connected_callback(self):
        """ After create connection to Websocket server successfully, we will subscribe orderbook event.
        """
        success, error = await self._init_symbol_info()
        if error:
            state = State("_init_symbol_info error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)
            #初始化过程中发生错误,关闭网络连接,触发重连机制
            await self.socket_close()
            return
        for symbol in self._symbols: #这里的symbol实际是Contract code
            info = self._syminfo[symbol]
            if not info: continue
            symbol_raw = info["symbol_raw"] #类似BTC_CQ这种表示方法
            self._s_to_cd[symbol_raw] = symbol
            #====================================
            if self.cb.on_kline_update_callback:
                channel = self._symbol_to_channel(symbol_raw, "kline")
                if channel:
                    kline = {"sub": channel}
                    await self.send_json(kline)
            #====================================
            if self.cb.on_orderbook_update_callback:
                channel = self._symbol_to_channel(symbol_raw, "depth")
                if channel:
                    data = {"sub": channel}
                    await self.send_json(data)
            #====================================
            if self.cb.on_trade_update_callback:
                channel = self._symbol_to_channel(symbol_raw, "trade")
                if channel:
                    data = {"sub": channel}
                    await self.send_json(data)

    async def process_binary(self, msg):
        """ Process binary message that received from Websocket connection.

        Args:
            msg: Binary message.
        """
        data = json.loads(gzip.decompress(msg).decode())
        # logger.debug("data:", json.dumps(data), caller=self)
        channel = data.get("ch")
        if not channel:
            if data.get("ping"):
                hb_msg = {"pong": data.get("ping")}
                await self.send_json(hb_msg)
            return

        symbol_raw = self._c_to_s[channel]
        symbol = self._s_to_cd[symbol_raw]

        if channel.find("kline") != -1:
            d = data.get("tick")
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "open": d["open"],
                "high": d["high"],
                "low": d["low"],
                "close": d["close"],
                "volume": d["amount"],
                "timestamp": data.get("ts"),
                "kline_type": MARKET_TYPE_KLINE
            }
            kline = Kline(**info)
            SingleTask.run(self.cb.on_kline_update_callback, kline)
        elif channel.find("depth") != -1:
            tick = data.get("tick")
            asks = tick.get("asks")[:self._orderbook_length]
            bids = tick.get("bids")[:self._orderbook_length]
            timestamp = tick.get("ts")
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": timestamp
            }
            ob = Orderbook(**info)
            SingleTask.run(self.cb.on_orderbook_update_callback, ob)
        elif channel.find("trade") != -1:
            tick = data.get("tick")
            for t in tick["data"]:
                direction = t.get("direction")
                price = t.get("price")
                quantity = t.get("amount")
                info = {
                    "platform": self._platform,
                    "symbol": symbol,
                    "action": ORDER_ACTION_BUY if direction == "buy" else ORDER_ACTION_SELL,
                    "price": price,
                    "quantity": quantity,
                    "timestamp": tick.get("ts")
                }
                trade = Trade(**info)
                SingleTask.run(self.cb.on_trade_update_callback, trade)
        else:
            logger.error("event error! msg:", msg, caller=self)

    def _symbol_to_channel(self, symbol_raw, channel_type):
        if channel_type == "kline":
            channel = "market.{s}.kline.1min".format(s=symbol_raw)
        elif channel_type == "depth":
            channel = "market.{s}.depth.step6".format(s=symbol_raw)
        elif channel_type == "trade":
            channel = "market.{s}.trade.detail".format(s=symbol_raw)
        else:
            logger.error("channel type error! channel type:", channel_type, calle=self)
            return None
        self._c_to_s[channel] = symbol_raw
        return channel