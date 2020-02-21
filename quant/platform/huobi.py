# -*- coding:utf-8 -*-

"""
huobi Trade module.
https://huobiapi.github.io/docs/spot/v1/cn

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import json
import hmac
import copy
import gzip
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
from quant.const import MARKET_TYPE_KLINE, INDICATE_ORDER, INDICATE_ASSET, INDICATE_POSITION
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


__all__ = ("HuobiRestAPI", "HuobiTrader", )


class HuobiRestAPI:
    """ huobi REST API 封装
    """

    def __init__(self, host, access_key, secret_key):
        """ 初始化
        @param host 请求host
        @param access_key API KEY
        @param secret_key SECRET KEY
        """
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key
        self._account_id = None

    async def get_symbols_info(self):
        """ 获取所有交易对基础信息
        @return data list 所有交易对基础信息
        """
        return await self.request("GET", "/v1/common/symbols")

    async def get_server_time(self):
        """ 获取服务器时间
        @return data int 服务器时间戳(毫秒)
        """
        return await self.request("GET", "/v1/common/timestamp")

    async def get_user_accounts(self):
        """ 获取账户信息
        """
        return await self.request("GET", "/v1/account/accounts", auth=True)

    async def get_account_id(self):
        """ 获取账户id
        """
        if self._account_id:
            return self._account_id
        success, error = await self.get_user_accounts()
        if error:
            return None
        for item in success:
            if item["type"] == "spot":
                self._account_id = item["id"]
                return self._account_id
        return None

    async def get_account_balance(self):
        """ 获取账户信息
        """
        account_id = await self.get_account_id()
        uri = "/v1/account/accounts/{account_id}/balance".format(account_id=account_id)
        return await self.request("GET", uri, auth=True)

    async def get_balance_all(self):
        """ 母账户查询其下所有子账户的各币种汇总余额
        """
        return await self.request("GET", "/v1/subuser/aggregate-balance", auth=True)

    async def create_order(self, symbol, price, quantity, order_type):
        """ 创建订单
        @param symbol 交易对
        @param quantity 交易量
        @param price 交易价格
        @param order_type 订单类型 buy-market, sell-market, buy-limit, sell-limit
        @return order_no 订单id
        """
        account_id = await self.get_account_id()
        info = {
            "account-id": account_id,
            "price": price,
            "amount": quantity,
            "source": "api",
            "symbol": symbol,
            "type": order_type
        }
        if order_type == "buy-market" or order_type == "sell-market":
            info.pop("price")
        return await self.request("POST", "/v1/order/orders/place", body=info, auth=True)

    async def revoke_order(self, order_no):
        """ 撤销委托单
        @param order_no 订单id
        @return True/False
        """
        uri = "/v1/order/orders/{order_no}/submitcancel".format(order_no=order_no)
        return await self.request("POST", uri, auth=True)

    async def revoke_orders(self, order_nos):
        """ 批量撤销委托单
        @param order_nos 订单列表
        * NOTE: 单次不超过50个订单id
        """
        body = {
            "order-ids": order_nos
        }
        return await self.request("POST", "/v1/order/orders/batchcancel", body=body, auth=True)

    async def get_open_orders(self, symbol):
        """ 获取当前还未完全成交的订单信息
        @param symbol 交易对
        * NOTE: 查询上限最多500个订单
        """
        account_id = await self.get_account_id()
        params = {
            "account-id": account_id,
            "symbol": symbol,
            "size": 500
        }
        return await self.request("GET", "/v1/order/openOrders", params=params, auth=True)

    async def get_order_status(self, order_no):
        """ 获取订单的状态
        @param order_no 订单id
        """
        uri = "/v1/order/orders/{order_no}".format(order_no=order_no)
        return await self.request("GET", uri, auth=True)

    async def request(self, method, uri, params=None, body=None, auth=False):
        """ 发起请求
        @param method 请求方法 GET POST
        @param uri 请求uri
        @param params dict 请求query参数
        @param body dict 请求body数据
        """
        url = urljoin(self._host, uri)
        params = params if params else {}

        if auth:
            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            params.update({"AccessKeyId": self._access_key,
                           "SignatureMethod": "HmacSHA256",
                           "SignatureVersion": "2",
                           "Timestamp": timestamp})
            host_name = urllib.parse.urlparse(self._host).hostname.lower()
            params["Signature"] = self.generate_signature(method, params, host_name, uri)

        if method == "GET":
            headers = {
                "Content-type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/39.0.2171.71 Safari/537.36"
            }
        else:
            headers = {
                "Accept": "application/json",
                "Content-type": "application/json"
            }
        _, success, error = await AsyncHttpRequests.fetch(method, url, params=params, data=body, headers=headers, timeout=10)
        if error:
            return success, error
        if success.get("status") != "ok":
            return None, success
        return success.get("data"), None

    def generate_signature(self, method, params, host_url, request_path):
        """ 创建签名
        """
        query = "&".join(["{}={}".format(k, parse.quote(str(params[k]))) for k in sorted(params.keys())])
        payload = [method, host_url, request_path, query]
        payload = "\n".join(payload)
        payload = payload.encode(encoding="utf8")
        secret_key = self._secret_key.encode(encoding="utf8")
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature


class HuobiTrader(Websocket, ExchangeGateway):
    """ huobi Trade模块
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
        self._host = "https://api.huobi.io"
        self._wss = "wss://api.huobi.io"
        
        url = self._wss + "/ws/v1"
        super(HuobiTrader, self).__init__(url, send_hb_interval=0, **kwargs)
        #self.heartbeat_msg = "ping"

        # Initializing our REST API client.
        self._rest_api = HuobiRestAPI(self._host, self._access_key, self._secret_key)

        self._account_id = None

        self._syminfo:DefaultDict[str: Dict[str, Any]] = defaultdict(dict)

        self._orders:DefaultDict[str: Dict[str, Order]] = defaultdict(dict)

        #e.g. {"BTC": {"free": 1.1, "locked": 2.2, "total": 3.3}, ... }
        self._assets: DefaultDict[str: Dict[str, float]] = defaultdict(lambda: {k: 0.0 for k in {'free', 'locked', 'total'}})

        """
        可以订阅两种订单更新频道,新方式和旧方式
        新方式:
               优点:延时小,大约100毫秒,不乱序,不丢包.
               缺点:包含信息量不全面,需要程序自己维护上下文状态才能获取完整信息.
        旧方式:
               优点:信息包含全面,程序不需要自己维护上下文状态.
               缺点:延时大,大约270毫秒,乱序,可能丢包(比如服务重启的时候).
        """
        self._use_old_style_order_channel = False #默认用新方式订阅
        self._order_channel = []
        for sym in self._symbols:
            if self._use_old_style_order_channel:
                self._order_channel.append("orders.{}".format(sym))
            else:
                self._order_channel.append("orders.{}.update".format(sym))

        if self._account != None:
            self.initialize()

        #市场行情数据
        HuobiMarket(**kwargs)

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ 创建订单
        @param symbol 交易对
        @param action 交易方向 BUY / SELL
        @param price 委托价格
        @param quantity 委托数量
        @param order_type 委托类型 LIMIT / MARKET
        """
        if action == ORDER_ACTION_BUY:
            if order_type == ORDER_TYPE_LIMIT:
                t = "buy-limit"
            elif order_type == ORDER_TYPE_MARKET:
                t = "buy-market"
            elif order_type == ORDER_TYPE_IOC:
                t = "buy-ioc"
            else:
                logger.error("order_type error! order_type:", order_type, caller=self)
                return None, "order type error"
        elif action == ORDER_ACTION_SELL:
            if order_type == ORDER_TYPE_LIMIT:
                t = "sell-limit"
            elif order_type == ORDER_TYPE_MARKET:
                t = "sell-market"
            elif order_type == ORDER_TYPE_IOC:
                t = "sell-ioc"
            else:
                logger.error("order_type error! order_type:", order_type, caller=self)
                return None, "order type error"
        else:
            logger.error("action error! action:", action, caller=self)
            return None, "action error"
        price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)
        result, error = await self._rest_api.create_order(symbol, price, quantity, t)
        #=====================================================
        #是否订阅的是新的订单更新频道
        if not self._use_old_style_order_channel:
            #如果下单成功,将新订单保存到缓存里
            if error == None:
                order_no = result
                tm = tools.get_cur_timestamp_ms()
                o = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "order_no": order_no,
                    "action": action,
                    "symbol": symbol,
                    "price": float(price),
                    "quantity": float(quantity),
                    "remain": float(quantity),
                    "status": ORDER_STATUS_SUBMITTED,
                    "order_type": order_type,
                    "ctime": tm,
                    "utime": tm
                    #avg_price
                }
                order = Order(**o)
                self._orders[symbol][order_no] = order
        #=====================================================
        return result, error

    async def revoke_order(self, symbol, *order_nos):
        """ 撤销订单
        @param symbol 交易对
        @param order_nos 订单号列表，可传入任意多个，如果不传入，那么就撤销所有订单
        备注:关于批量删除订单函数返回值格式,如果函数调用失败了那肯定是return None, error
        如果函数调用成功,但是多个订单有成功有失败的情况,比如输入3个订单id,成功2个,失败1个,那么
        返回值统一都类似: 
        return [(成功订单ID, None),(成功订单ID, None),(失败订单ID, "失败原因")], None
        """
        # 如果传入order_nos为空，即撤销全部委托单
        if len(order_nos) == 0:
            order_nos, error = await self.get_orders(symbol)
            if error:
                return [], error
            if not order_nos:
                return [], None

        # 如果传入order_nos为一个委托单号，那么只撤销一个委托单
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        # 如果传入order_nos数量大于1，那么就批量撤销传入的委托单
        if len(order_nos) > 1:
            """
            {
            "status": "ok",
            "data": {
                "success": [
                    "5983466"
                ],
                "failed": [
                    {
                      "err-msg": "Incorrect order state",
                      "order-state": 7,
                      "order-id": "",
                      "err-code": "order-orderstate-error",
                      "client-order-id": "first"
                    },
                    {
                      "err-msg": "The record is not found.",
                      "order-id": "",
                      "err-code": "base-not-found",
                      "client-order-id": "second"
                    }
                  ]
                }
            }
            """
            s, e = await self._rest_api.revoke_orders(order_nos)
            if e:
                return [], e
            result = []
            for x in s["success"]:
                result.append((x, None))
            for x in s["failed"]:
                result.append((x["order-id"], x["err-msg"]))
            return result, None

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        #{"status": "ok", "data": [{"filled-cash-amount": "0.0", "filled-fees": "0.0", "filled-amount": "0.0", "symbol": "trxeth", "source": "web", "created-at": 1575100309209, "amount": "17.000000000000000000", "account-id": 11261082, "price": "0.000100000000000000", "id": 58040174635, "state": "submitted", "type": "buy-limit"}, {"filled-cash-amount": "0.0", "filled-fees": "0.0", "filled-amount": "0.0", "symbol": "trxeth", "source": "web", "created-at": 1575018429010, "amount": "10.000000000000000000", "account-id": 11261082, "price": "0.000100000000000000", "id": 57906933472, "state": "submitted", "type": "buy-limit"}]}
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
        #{"status": "ok", "data": {"id": 11261082, "type": "spot", "state": "working", "list": [{"currency": "lun", "type": "trade", "balance": "0"}, {"currency": "lun", "type": "frozen", "balance": "0"}]}}
        success, error = await self._rest_api.get_account_balance()
        if error:
            return None, error
        assets: DefaultDict[str: Dict[str, float]] = defaultdict(lambda: {k: 0.0 for k in {'free', 'locked', 'total'}})
        for d in success["list"]:
            b = d["balance"]
            if b == "0": continue
            c = d["currency"]
            t = d["type"]
            if t == "trade":
                assets[c]["free"] = float(b)
            elif t == "frozen":
                assets[c]["locked"] = float(b)
        for (_, v) in assets.items():
            v["total"] = v["free"] + v["locked"]
        ast = Asset(self._platform, self._account, assets, tools.get_cur_timestamp_ms(), True)
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
        [{
        symbol-partition = "main", #交易区，可能值: [main，innovation]
        symbol = "trxeth", #交易对
        state = "online", #交易对状态；可能值: [online，offline,suspend] online - 已上线；offline - 交易对已下线，不可交易；suspend -- 交易暂停
        base-currency = "trx", #交易对中的基础币种
        quote-currency = "eth", #交易对中的报价币种
        price-precision = 8,   #交易对报价的精度（小数点后位数）
        amount-precision = 2,  #交易对基础币种计数精度（小数点后位数）
        value-precision = 8, #交易对交易金额的精度（小数点后位数）
        min-order-amt = 1, #交易对最小下单量 (下单量指当订单类型为限价单或sell-market时，下单接口传的'amount')
        max-order-amt = 10000000, #交易对最大下单量
        min-order-value = 0.001, #最小下单金额 （下单金额指当订单类型为限价单时，下单接口传入的(amount * price)。当订单类型为buy-market时，下单接口传的'amount'）
        #"leverage-ratio":4 #交易对杠杆最大倍数(杠杆交易才有这个字段)
        },]
        """
        info = self._syminfo[symbol]
        if not info:
            return None, "Symbol not exist"
        price_tick = 1/float(10**info["price-precision"])
        size_tick = 1/float(10**info["amount-precision"])
        size_limit = info["min-order-amt"]
        value_tick = 1/float(10**info["value-precision"])
        value_limit = info["min-order-value"]
        base_currency = info["base-currency"]
        quote_currency = info["quote-currency"]
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
        """ 建立连接之后，授权登陆，然后订阅相关频道等
        """
        #进行登录认证,然后订阅需要登录后才能订阅的私有频道
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        params = {
            "AccessKeyId": self._access_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": timestamp
        }
        signature = self._rest_api.generate_signature("GET", params, "api.huobi.io", "/ws/v1")
        params["op"] = "auth"
        params["Signature"] = signature
        await self.send_json(params)

    async def _auth_success_callback(self):
        """ 授权成功之后回调
        """
        #获取现货账户ID
        self._account_id = await self._rest_api.get_account_id()
        if self._account_id == None:
            state = State("get_account_id error", State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return
        
        #获取相关符号信息
        success, error = await self._rest_api.get_symbols_info()
        if error:
            state = State("get_symbols_info error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return
        for info in success:
            self._syminfo[info["symbol"]] = info #符号信息一般不变,获取一次保存好,其他地方要用直接从这个变量获取就可以了

        #获取账户余额，更新资产
        #{"status": "ok", "data": {"id": 11261082, "type": "spot", "state": "working", "list": [{"currency": "lun", "type": "trade", "balance": "0"}, {"currency": "lun", "type": "frozen", "balance": "0"}]}}
        success, error = await self._rest_api.get_account_balance()
        if error:
            state = State("get_account_balance error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return
        for d in success["list"]:
            b = d["balance"]
            if b == "0": continue
            c = d["currency"]
            t = d["type"]
            if t == "trade":
                self._assets[c]["free"] = float(b)
            #elif t == "frozen":
            #    self._assets[c]["locked"] = b
        ast = Asset(self._platform, self._account, self._assets, tools.get_cur_timestamp_ms(), True)
        if self.cb.on_asset_update_callback:
            SingleTask.run(self.cb.on_asset_update_callback, ast)

        #获取当前未完成订单
        for sym in self._symbols:
            success, error = await self.get_orders(sym)
            if error:
                state = State("get_orders error: {}".format(error), State.STATE_CODE_GENERAL_ERROR)
                SingleTask.run(self.cb.on_state_update_callback, state)
                return
            for order in success:
                #是否订阅的是新的订单更新频道
                if not self._use_old_style_order_channel:
                    self._orders[sym][order.order_no] = order
                if self.cb.on_order_update_callback:
                    SingleTask.run(self.cb.on_order_update_callback, order)

        #订阅账号资产信息
        if self.cb.on_asset_update_callback:
            params = {
                "op": "sub",
                "topic": "accounts",
                "model": "0"
            }
            await self.send_json(params)

        #订阅订单更新频道
        if self.cb.on_order_update_callback or self.cb.on_fill_update_callback:
            for ch in self._order_channel:
                params = {
                    "op": "sub",
                    "topic": ch
                }
                await self.send_json(params)

        #计数初始化0
        self._subscribe_response_count = 0

    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
        """
        #{'op': 'error', 'ts': 1575003013045, 'err-code': 1002, 'err-msg': 'internal error : auth not received.'}
        #{'op': 'close', 'ts': 1575003013045}
        #{'op': 'auth', 'ts': 1575003739511, 'err-code': 0, 'data': {'user-id': 12053842}}
        #{'op': 'ping', 'ts': 1575003876880}
        #{'op': 'sub', 'ts': 1575003877414, 'topic': 'orders.eoseth.update', 'err-code': 0}
        #{'op': 'sub', 'ts': 1575003882668, 'topic': 'orders.trxeth.update', 'err-code': 0}
        #{'op': 'sub', 'ts': 1575003888499, 'topic': 'accounts', 'err-code': 0
        #==创建订单:
        #{'op': 'notify', 'ts': 1575004328706, 'topic': 'accounts', 'data': {'event': 'order.place', 'list': [{'account-id': 10432498, 'currency': 'eth', 'type': 'trade', 'balance': '0.71662865'}]}}
        #{'op': 'notify', 'ts': 1575004328733, 'topic': 'orders.trxeth.update', 'data': {'role': 'taker', 'match-id': 100413368307, 'filled-cash-amount': '0', 'filled-amount': '0', 'price': '0.0001', 'order-id': 57886011451, 'client-order-id': '', 'order-type': 'buy-limit', 'unfilled-amount': '10', 'symbol': 'trxeth', 'order-state': 'submitted'}}
        #==撤销订单:
        #{'op': 'notify', 'ts': 1575004686930, 'topic': 'orders.trxeth.update', 'data': {'role': 'taker', 'match-id': 100413372769, 'filled-cash-amount': '0', 'filled-amount': '0', 'price': '0.0001', 'order-id': 57886011451, 'client-order-id': '', 'order-type': 'buy-limit', 'unfilled-amount': '10', 'symbol': 'trxeth', 'order-state': 'canceled'}}
        #{'op': 'notify', 'ts': 1575004687037, 'topic': 'accounts', 'data': {'event': 'order.cancel', 'list': [{'account-id': 10432498, 'currency': 'eth', 'type': 'trade', 'balance': '0.71762865'}]}}
        msg = json.loads(gzip.decompress(raw).decode())
        logger.debug("msg:", msg, caller=self)
        op = msg.get("op")
        if op == "auth":  # 授权
            if msg["err-code"] != 0:
                state = State("Websocket connection authorized failed: {}".format(msg), State.STATE_CODE_GENERAL_ERROR)
                logger.error(state, caller=self)
                SingleTask.run(self.cb.on_state_update_callback, state)
                return
            logger.info("Websocket connection authorized successfully.", caller=self)
            await self._auth_success_callback()
        elif op == "error":  # error
            state = State("Websocket error: {}".format(msg), State.STATE_CODE_GENERAL_ERROR)
            logger.error(state, caller=self)
            SingleTask.run(self.cb.on_state_update_callback, state)
        elif op == "close":  # close
            return
        elif op == "ping":  # ping
            params = {
                "op": "pong",
                "ts": msg["ts"]
            }
            await self.send_json(params)
        elif op == "sub":   # 返回订阅操作是否成功
            exist = False
            for ch in self._order_channel:
                if msg["topic"] == ch:
                    exist = True
                    break
            if msg["topic"] == "accounts":
                exist = True
            if not exist:
                return
            if msg["err-code"] == 0:
                self._subscribe_response_count = self._subscribe_response_count + 1 #每来一次订阅响应计数就加一
                count = len(self._order_channel)+1 #应该要返回的订阅响应数
                if self._subscribe_response_count == count: #所有的订阅都成功了,通知上层接口都准备好了
                    state = State("Environment ready", State.STATE_CODE_READY)
                    SingleTask.run(self.cb.on_state_update_callback, state)
            else:
                state = State("subscribe event error: {}".format(msg), State.STATE_CODE_GENERAL_ERROR)
                logger.error(state, caller=self)
                SingleTask.run(self.cb.on_state_update_callback, state)
        elif op == "notify":  # 频道更新通知
            for ch in self._order_channel:
                if msg["topic"] == ch:
                    if self._use_old_style_order_channel:
                        self._update_order_and_fill_old_style(msg)
                    else:
                        self._update_order_and_fill(msg)
                    break
            if msg["topic"] == "accounts":
                self._update_asset(msg)

    def _convert_order_format(self, order_info):
        symbol = order_info["symbol"]
        order_no = str(order_info["id"])
        remain = float(order_info["amount"]) - float(order_info["filled-amount"])
        action = ORDER_ACTION_BUY if order_info["type"] in ["buy-market", "buy-limit", "buy-ioc", "buy-limit-maker", "buy-stop-limit"] else ORDER_ACTION_SELL
        if order_info["type"] in ["buy-market", "sell-market"]:
            order_type = ORDER_TYPE_MARKET
        elif order_info["type"] in ["buy-ioc", "sell-ioc"]:
            order_type = ORDER_TYPE_IOC
        else:
            order_type = ORDER_TYPE_LIMIT
        ctime = order_info["created-at"]
        utime = order_info["created-at"]
        state = order_info["state"]
        if state == "canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "partial-canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "created":
            status = ORDER_STATUS_SUBMITTED
        elif state == "submitting":
            status = ORDER_STATUS_SUBMITTED
        elif state == "submitted":
            status = ORDER_STATUS_SUBMITTED
        elif state == "partical-filled":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "filled":
            status = ORDER_STATUS_FILLED
        else:
            logger.error("status error! order_info:", order_info, caller=self)
            status = ORDER_STATUS_NONE
        info = {
            "platform": self._platform,
            "account": self._account,
            "strategy": self._strategy,
            "order_no": order_no,
            "action": action,
            "symbol": symbol,
            "price": float(order_info["price"]),
            "quantity": float(order_info["amount"]),
            "remain": remain,
            "status": status,
            "order_type": order_type,
            "ctime": ctime,
            "utime": utime
            #avg_price
        }
        return Order(**info)

    def _update_fill(self, order_info, ctime):
        """处理成交通知
        """
        symbol = order_info["symbol"]
        order_no = str(order_info["order-id"])
        fill_no = str(order_info["match-id"])
        price = float(order_info["price"]) #成交价格
        size = float(order_info["filled-amount"]) #成交数量
        side = ORDER_ACTION_BUY if order_info["order-type"] in ["buy-market", "buy-limit", "buy-ioc", "buy-limit-maker", "buy-stop-limit"] else ORDER_ACTION_SELL
        liquidity = LIQUIDITY_TYPE_TAKER if order_info["role"]=="taker" else LIQUIDITY_TYPE_MAKER
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
            #"fee": fee, #通知里没提供,所以只能注释,或者也可以自己算
            "ctime": ctime
        }
        fill = Fill(**f)
        if self.cb.on_fill_update_callback:
            SingleTask.run(self.cb.on_fill_update_callback, fill)

    def _update_order_and_fill(self, msg):
        """
        {
        'op': 'notify', 
        'ts': 1575004328733, 
        'topic': 'orders.trxeth.update', 
        'data': {
                  'role': 'taker', #最近成交角色（当order-state = submitted, canceled, partial-canceled时，role 为缺省值taker；当order-state = filled, partial-filled 时，role 取值为taker 或maker。）
                  'match-id': 100413368307, #最近撮合编号（当order-state = submitted, canceled, partial-canceled时，match-id 为消息序列号；当order-state = filled, partial-filled 时，match-id 为最近撮合编号。）
                  'filled-cash-amount': '0', #最近成交数额
                  'filled-amount': '0', #最近成交数量
                  'price': '0.0001', #最新价（当order-state = submitted 时，price 为订单价格；当order-state = canceled, partial-canceled 时，price 为零；当order-state = filled, partial-filled 时，price 为最近成交价。）
                  'order-id': 57886011451, #订单编号
                  'client-order-id': '', #用户自编订单号
                  'order-type': 'buy-limit', #订单类型，包括buy-market, sell-market, buy-limit, sell-limit, buy-ioc, sell-ioc, buy-limit-maker, sell-limit-maker, buy-stop-limit, sell-stop-limit
                  'unfilled-amount': '10', #最近未成交数量（当order-state = submitted 时，unfilled-amount 为原始订单量；当order-state = canceled OR partial-canceled 时，unfilled-amount 为未成交数量；当order-state = filled 时，如果 order-type = buy-market，unfilled-amount 可能为一极小值；如果order-type <> buy-market 时，unfilled-amount 为零；当order-state = partial-filled AND role = taker 时，unfilled-amount 为未成交数量；当order-state = partial-filled AND role = maker 时，unfilled-amount 为未成交数量。）
                  'symbol': 'trxeth',  #交易代码
                  'order-state': 'submitted' #订单状态, 有效取值: submitted, partial-filled, filled, canceled, partial-canceled
                }
        }
        """
        order_info = msg["data"]
        state = order_info["order-state"]
        if state == "canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "partial-canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "created":
            status = ORDER_STATUS_SUBMITTED
        elif state == "submitting":
            status = ORDER_STATUS_SUBMITTED
        elif state == "submitted":
            status = ORDER_STATUS_SUBMITTED
        elif state == "partical-filled":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "filled":
            status = ORDER_STATUS_FILLED
        else:
            logger.error("status error! order_info:", order_info, caller=self)
            return
        symbol = order_info["symbol"]
        order_no = str(order_info["order-id"])
        remain = float(order_info["unfilled-amount"])
        action = ORDER_ACTION_BUY if order_info["order-type"] in ["buy-market", "buy-limit", "buy-ioc", "buy-limit-maker", "buy-stop-limit"] else ORDER_ACTION_SELL
        if order_info["type"] in ["buy-market", "sell-market"]:
            order_type = ORDER_TYPE_MARKET
        elif order_info["type"] in ["buy-ioc", "sell-ioc"]:
            order_type = ORDER_TYPE_IOC
        else:
            order_type = ORDER_TYPE_LIMIT
        #tm = msg["ts"]
        tm = tools.get_cur_timestamp_ms()
        order = self._orders[symbol].get(order_no)
        if order == None:
            return #如果收到的订单通知在缓存中不存在的话就直接忽略不处理
        order.remain = remain
        order.status = status
        order.utime = tm
        if self.cb.on_order_update_callback:
            SingleTask.run(self.cb.on_order_update_callback, order)
        if status in [ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders[symbol].pop(order_no) #这个订单完成了,从缓存里面删除
        #如果是成交通知,就处理成交回调
        if status == ORDER_STATUS_PARTIAL_FILLED or status == ORDER_STATUS_FILLED:
            self._update_fill(order_info, tm)

    def _update_order_and_fill_old_style(self, msg):
        """ 更新订单信息
        @param msg 订单信息
        """
        #{'op': 'notify', 'ts': 1575268899866, 'topic': 'orders.trxeth', 'data': {'seq-id': 100418110944, 'order-id': 58326818953, 'symbol': 'trxeth', 'account-id': 11261082, 'order-amount': '10', 'order-price': '0.000104', 'created-at': 1575268899682, 'order-type': 'buy-limit', 'order-source': 'spot-web', 'order-state': 'filled', 'role': 'taker', 'price': '0.00010399', 'filled-amount': '10', 'unfilled-amount': '0', 'filled-cash-amount': '0.0010399', 'filled-fees': '0.02'}}
        #{'op': 'notify', 'ts': 1575269220762, 'topic': 'orders.trxeth', 'data': {'seq-id': 100418116512, 'order-id': 58324882527, 'symbol': 'trxeth', 'account-id': 11261082, 'order-amount': '10', 'order-price': '0.00010376', 'created-at': 1575269220597, 'order-type': 'buy-limit', 'order-source': 'spot-web', 'order-state': 'canceled', 'role': 'taker', 'price': '0.00010376', 'filled-amount': '0', 'unfilled-amount': '10', 'filled-cash-amount': '0', 'filled-fees': '0'}}
        #{'op': 'notify', 'ts': 1575269259564, 'topic': 'orders.trxeth', 'data': {'seq-id': 100418116991, 'order-id': 58327457834, 'symbol': 'trxeth', 'account-id': 11261082, 'order-amount': '9.98', 'order-price': '0', 'created-at': 1575269259451, 'order-type': 'sell-market', 'order-source': 'spot-web', 'order-state': 'filled', 'role': 'taker', 'price': '0.00010407', 'filled-amount': '9.98', 'unfilled-amount': '0', 'filled-cash-amount': '0.0010386186', 'filled-fees': '0.0000020772372'}}
        #{'op': 'notify', 'ts': 1575269323862, 'topic': 'orders.trxeth', 'data': {'seq-id': 100418118242, 'order-id': 58327583981, 'symbol': 'trxeth', 'account-id': 11261082, 'order-amount': '0.001', 'order-price': '0', 'created-at': 1575269323654, 'order-type': 'buy-market', 'order-source': 'spot-web', 'order-state': 'filled', 'role': 'taker', 'price': '0.00010425', 'filled-amount': '9.59232613908872901', 'unfilled-amount': '0', 'filled-cash-amount': '0.000999999999999999', 'filled-fees': '0.019184652278177458'}}
        """
        {
        'op': 'notify', 
        'ts': 1575269323862, 
        'topic': 'orders.trxeth', 
        'data': {
                'seq-id': 100418118242, 
                'order-id': 58327583981, 
                'symbol': 'trxeth', 
                'account-id': 11261082, 
                'order-amount': '0.001', 
                'order-price': '0', 
                'created-at': 1575269323654, 
                'order-type': 'buy-market', 
                'order-source': 'spot-web', 
                'order-state': 'filled', 
                'role': 'taker', 
                'price': '0.00010425', 
                'filled-amount': '9.59232613908872901', 
                'unfilled-amount': '0', 
                'filled-cash-amount': '0.000999999999999999', 
                'filled-fees': '0.019184652278177458'}
        }
        """
        tm = msg["ts"]
        order_info = msg["data"]
        symbol = order_info["symbol"]
        order_no = str(order_info["order-id"])
        remain = float(order_info["unfilled-amount"])
        action = ORDER_ACTION_BUY if order_info["order-type"] in ["buy-market", "buy-limit", "buy-ioc", "buy-limit-maker", "buy-stop-limit"] else ORDER_ACTION_SELL
        if order_info["type"] in ["buy-market", "sell-market"]:
            order_type = ORDER_TYPE_MARKET
        elif order_info["type"] in ["buy-ioc", "sell-ioc"]:
            order_type = ORDER_TYPE_IOC
        else:
            order_type = ORDER_TYPE_LIMIT
        ctime = order_info["created-at"]
        utime = tm
        state = order_info["order-state"]
        if state == "canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "partial-canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "created":
            status = ORDER_STATUS_SUBMITTED
        elif state == "submitting":
            status = ORDER_STATUS_SUBMITTED
        elif state == "submitted":
            status = ORDER_STATUS_SUBMITTED
        elif state == "partical-filled":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "filled":
            status = ORDER_STATUS_FILLED
        else:
            logger.error("status error! order_info:", order_info, caller=self)
            return
        info = {
            "platform": self._platform,
            "account": self._account,
            "strategy": self._strategy,
            "order_no": order_no,
            "action": action,
            "symbol": symbol,
            "price": float(order_info["order-price"]),
            "quantity": float(order_info["order-amount"]),
            "remain": remain,
            "status": status,
            "order_type": order_type,
            "ctime": ctime,
            "utime": utime
            #avg_price
        }
        order = Order(**info)
        if self.cb.on_order_update_callback:
            SingleTask.run(self.cb.on_order_update_callback, order)
        #=====================================================================================
        #接下来处理成交回调
        fill_no = str(order_info["seq-id"])
        price = float(order_info["price"]) #成交价格
        size = float(order_info["filled-amount"]) #成交数量
        side = action
        liquidity = LIQUIDITY_TYPE_TAKER if order_info["role"]=="taker" else LIQUIDITY_TYPE_MAKER
        fee = float(order_info["filled-fees"])
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
            "ctime": tm
        }
        fill = Fill(**f)
        if self.cb.on_fill_update_callback:
            SingleTask.run(self.cb.on_fill_update_callback, fill)
 
    def _update_asset(self, msg):
        """
        {
        'op': 'notify', 
        'ts': 1575004687037, 
        'topic': 'accounts', 
        'data': {
                 'event': 'order.cancel', #资产变化通知相关事件说明，比如订单创建(order.place) 、订单成交(order.match)、订单成交退款（order.refund)、订单撤销(order.cancel) 、点卡抵扣交易手续费（order.fee-refund)、杠杆账户划转（margin.transfer)、借币本金（margin.loan)、借币计息（margin.interest)、归还借币本金利息(margin.repay)、其他资产变化(other)
                 'list': [
                          {
                          'account-id': 10432498, #账户id
                          'currency': 'eth', #币种
                          'type': 'trade', #交易（trade),借币（loan），利息（interest)
                          'balance': '0.71762865' #账户余额 (当订阅model=0时，该余额为可用余额；当订阅model=1时，该余额为总余额）
                          }
                  ]
             }
        }
        """
        tm = msg["ts"]
        account_info = msg["data"]
        for d in account_info["list"]:
            b = d["balance"]
            c = d["currency"]
            self._assets[c]["free"] = float(b)
        ast = Asset(self._platform, self._account, self._assets, tm, True)
        SingleTask.run(self.cb.on_asset_update_callback, ast)


class HuobiMarket(Websocket):
    """ Huobi Market Server.
    """

    def __init__(self, **kwargs):
        self.cb = kwargs["cb"]
        self._platform = kwargs["platform"]
        self._symbols = kwargs["symbols"]
        self._wss = "wss://api.huobi.io"
        url = self._wss + "/ws"
        super(HuobiMarket, self).__init__(url, send_hb_interval=0, **kwargs)
        #self.heartbeat_msg = "ping"
        self._c_to_s = {}  # {"channel": "symbol"}
        self.initialize()

    async def connected_callback(self):
        """After create Websocket connection successfully, we will subscribing orderbook/trade/kline events."""
        if self.cb.on_kline_update_callback:
            for symbol in self._symbols:
                channel = self._symbol_to_channel(symbol, "kline")
                if channel:
                    data = {"sub": channel}
                    await self.send_json(data)

        if self.cb.on_orderbook_update_callback:
            for symbol in self._symbols:
                channel = self._symbol_to_channel(symbol, "depth")
                if channel:
                    data = {"sub": channel}
                    await self.send_json(data)

        if self.cb.on_trade_update_callback:
            for symbol in self._symbols:
                channel = self._symbol_to_channel(symbol, "trade")
                if channel:
                    data = {"sub": channel}
                    await self.send_json(data)

    async def process_binary(self, raw):
        """ Process binary message that received from Websocket connection.

        Args:
            raw: Binary message received from Websocket connection.
        """
        data = json.loads(gzip.decompress(raw).decode())
        logger.debug("data:", json.dumps(data), caller=self)
        channel = data.get("ch")
        if not channel:
            if data.get("ping"):
                hb_msg = {"pong": data.get("ping")}
                await self.send_json(hb_msg)
            return

        symbol = self._c_to_s[channel]

        if channel.find("kline") != -1:
            kline_info = data["tick"]
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "open": kline_info["open"],
                "high": kline_info["high"],
                "low": kline_info["low"],
                "close": kline_info["close"],
                "volume": kline_info["amount"],
                "timestamp": data["ts"],
                "kline_type": MARKET_TYPE_KLINE
            }
            kline = Kline(**info)
            SingleTask.run(self.cb.on_kline_update_callback, kline)
        elif channel.find("depth") != -1:
            d = data["tick"]
            asks = d["asks"][:20] #[[price, quantity],....]
            bids = d["bids"][:20]
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": d["ts"]
            }
            ob = Orderbook(**info)
            SingleTask.run(self.cb.on_orderbook_update_callback, ob)
        elif channel.find("trade") != -1:
            tick = data["tick"]
            for t in tick["data"]:
                info = {
                    "platform": self._platform,
                    "symbol": symbol,
                    "action": ORDER_ACTION_BUY if t["direction"] == "buy" else ORDER_ACTION_SELL,
                    "price": t["price"],
                    "quantity": t["amount"],
                    "timestamp": t["ts"]
                }
                trade = Trade(**info)
                SingleTask.run(self.cb.on_trade_update_callback, trade)

    def _symbol_to_channel(self, symbol, channel_type):
        """ Convert symbol to channel.

        Args:
            symbol: Trade pair name.
            channel_type: channel name, kline / trade / depth.
        """
        if channel_type == "kline":
            channel = "market.{s}.kline.1min".format(s=symbol)
        elif channel_type == "depth":
            channel = "market.{s}.depth.step0".format(s=symbol)
        elif channel_type == "trade":
            channel = "market.{s}.trade.detail".format(s=symbol)
        else:
            logger.error("channel type error! channel type:", channel_type, calle=self)
            return None
        self._c_to_s[channel] = symbol
        return channel
