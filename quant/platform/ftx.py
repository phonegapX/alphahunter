# -*- coding:utf-8 -*-

"""
FTX Trade module.
https://docs.ftx.com/

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import time
import zlib
import json
import copy
import hmac
import base64
from urllib.parse import urljoin
from collections import defaultdict, deque
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional, Any
from itertools import zip_longest
from requests import Request

from quant.gateway import ExchangeGateway
from quant.error import Error
from quant.order import Order, Fill, SymbolInfo
from quant.tasks import SingleTask, LoopRunTask
from quant.position import Position
from quant.asset import Asset
from quant.const import MARKET_TYPE_KLINE
from quant.utils import tools, logger
from quant.utils.websocket import Websocket
from quant.utils.http_client import AsyncHttpRequests
from quant.utils.decorator import async_method_locker
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET
from quant.order import LIQUIDITY_TYPE_MAKER, LIQUIDITY_TYPE_TAKER
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED
from quant.market import Kline, Orderbook, Trade, Ticker


__all__ = ("FTXRestAPI", "FTXTrader", )


class FTXRestAPI:
    """
    """

    def __init__(self, host, api_key=None, api_secret=None, subaccount_name=None) -> None:
        self._host = host
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name

    async def _request(self, method: str, path: str, **kwargs) -> Any:
        url = self._host + "/api/" + path
        request = Request(method, url, **kwargs)
        if self._api_key and self._api_secret:
            self._sign_request(request)
        _, success, error = await AsyncHttpRequests.fetch(method, url, headers=request.headers, timeout=10, **kwargs)
        return success, error

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = self._subaccount_name

    async def list_futures(self) -> List[dict]:
        return await self._request('GET', 'futures')

    async def get_future(self, market: str) -> dict:
        return await self._request('GET', f'futures/{market}')

    async def list_markets(self) -> List[dict]:
        return await self._request('GET', 'markets')

    async def get_orderbook(self, market: str, depth: int = None) -> dict:
        return await self._request('GET', f'markets/{market}/orderbook', params={'depth': depth})

    async def get_trades(self, market: str) -> dict:
        return await self._request('GET', f'markets/{market}/trades')

    async def get_account_info(self) -> dict:
        return await self._request('GET', 'account')

    async def get_open_orders(self, market: str = None) -> List[dict]:
        return await self._request('GET', 'orders', params={'market': market})

    async def get_conditional_orders(self, market: str = None) -> List[dict]:
        return await self._request('GET', 'conditional_orders', params={'market': market})

    async def place_order(self, market: str, side: str, price: float, size: float, type: str = 'limit',
                    reduce_only: bool = False, ioc: bool = False, post_only: bool = False,
                    client_id: str = None) -> dict:
        return await self._request('POST', 'orders', json={'market': market,
                                                           'side': side,
                                                           'price': price,
                                                           'size': size,
                                                           'type': type,
                                                           'reduceOnly': reduce_only,
                                                           'ioc': ioc,
                                                           'postOnly': post_only,
                                                           'clientId': client_id})

    async def place_conditional_order(
        self, market: str, side: str, size: float, type: str = 'stop',
        limit_price: float = None, reduce_only: bool = False, cancel: bool = True,
        trigger_price: float = None, trail_value: float = None) -> dict:
        """
        To send a Stop Market order, set type='stop' and supply a trigger_price
        To send a Stop Limit order, also supply a limit_price
        To send a Take Profit Market order, set type='trailing_stop' and supply a trigger_price
        To send a Trailing Stop order, set type='trailing_stop' and supply a trail_value
        """
        assert type in ('stop', 'take_profit', 'trailing_stop')
        assert type not in ('stop', 'take_profit') or trigger_price is not None, 'Need trigger prices for stop losses and take profits'
        assert type not in ('trailing_stop') or (trigger_price is None and trail_value is not None), 'Trailing stops need a trail value and cannot take a trigger price'
        return await self._request('POST', 'conditional_orders', json={'market': market, 
                                                                       'side': side, 
                                                                       'triggerPrice': trigger_price,
                                                                       'size': size, 
                                                                       'reduceOnly': reduce_only, 
                                                                       'type': 'stop',
                                                                       'cancelLimitOnTrigger': cancel, 
                                                                       'orderPrice': limit_price})

    async def cancel_order(self, order_id: str) -> dict:
        return await self._request('DELETE', f'orders/{order_id}')

    async def cancel_orders(self, market_name: str = None, conditional_orders: bool = False, limit_orders: bool = False) -> dict:
        return await self._request('DELETE', 'orders', json={'market': market_name,
                                                             'conditionalOrdersOnly': conditional_orders,
                                                             'limitOrdersOnly': limit_orders})

    async def get_fills(self) -> List[dict]:
        return await self._request('GET', 'fills')

    async def get_balances(self) -> List[dict]:
        return await self._request('GET', 'wallet/balances')

    async def get_deposit_address(self, ticker: str) -> dict:
        return await self._request('GET', f'wallet/deposit_address/{ticker}')

    async def get_positions(self, show_avg_price: bool = False) -> List[dict]:
        return await self._request('GET', 'positions', params={'showAvgPrice': str(show_avg_price)})

    async def get_kline(self, market_name: str, resolution: int, limit: int = None, start_time: int = None, end_time: int = None) -> dict:
        #GET /markets/{market_name}/candles?resolution={resolution}&limit={limit}&start_time={start_time}&end_time={end_time}
        params = {'resolution': resolution}
        if limit:
            params["limit"] = limit
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return await self._request('GET', f'markets/{market_name}/candles', params=params)

class FTXTrader(Websocket, ExchangeGateway):
    """ FTX Trade module. You can initialize trader object with some attributes in kwargs.
    """

    def __init__(self, **kwargs):
        """Initialize."""
        self.cb = kwargs["cb"]
        e = None
        if kwargs.get("account") and (not kwargs.get("access_key") or not kwargs.get("secret_key")):
            e = Error("param access_key or secret_key miss")
        elif not kwargs.get("strategy"):
            e = Error("param strategy miss")
        elif not kwargs.get("symbols"):
            e = Error("param symbols miss")
        elif not kwargs.get("platform"):
            e = Error("param platform miss")
            
        if e:
            logger.error(e, caller=self)
            SingleTask.run(self.cb.on_init_success_callback, False, e)
            return

        if not kwargs.get("host"):
            kwargs["host"] = "https://ftx.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://ftx.com"

        self._account = kwargs.get("account")
        self._access_key = kwargs.get("access_key")
        self._secret_key = kwargs.get("secret_key")
        self._subaccount_name = kwargs.get("subaccount_name")
        self._strategy = kwargs["strategy"]
        self._platform = kwargs["platform"]
        self._symbols = kwargs["symbols"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        
        url = self._wss + "/ws"
        super(FTXTrader, self).__init__(url, check_conn_interval=5, send_hb_interval=0)
        #self.heartbeat_msg = "ping"

        # Initializing our REST API client.
        self._rest_api = FTXRestAPI(self._host, self._access_key, self._secret_key, self._subaccount_name)
        
        self.raw_kwargs = {
            "platform": kwargs["platform"],
            "account": kwargs.get("account"),
            "symbols": kwargs["symbols"],
            "strategy": kwargs["strategy"]
        }
        
        #订单簿深度数据
        self._orderbooks: DefaultDict[str, Dict[str, DefaultDict[float, float]]] = defaultdict(lambda: {side: defaultdict(float) for side in {'bids', 'asks'}})
        
        self._assets = {}
        
        self.initialize()

    @property
    def rest_api(self):
        return self._rest_api
    
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

        #{"result": {"avgFillPrice": null, "clientId": null, "createdAt": "2019-11-16T11:08:37.726313+00:00", "filledSize": 0.0, "future": "ETH-PERP", "id": 871282987, "ioc": false, "market": "ETH-PERP", "postOnly": false, "price": 251.0, "reduceOnly": false, "remainingSize": 0.02, "side": "sell", "size": 0.02, "status": "new", "type": "limit"}, "success": true}

        if action == ORDER_ACTION_BUY:
            side = "buy"
        else:
            side = "sell"

        size = abs(float(quantity))
        price = float(price)
        
        if order_type == ORDER_TYPE_LIMIT:
            ot = "limit"
        else:
            ot = "market"

        success, error = await self._rest_api.place_order(symbol, side, price, size, ot)
        if error:
            return None, error
        
        if not success["success"]:
            return None, "place_order error"
        
        result = success["result"]
        
        return result["id"], None

    async def revoke_order(self, symbol, *order_nos):
        """ Revoke (an) order(s).

        Args:
            symbol: Trade target
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel all orders for 
            this symbol. If you set 1 or multiple param, you can cancel an or multiple order.

        Returns:
            删除全部订单情况: 成功=(True, None), 失败=(False, error information)
            删除单个或多个订单情况: (删除成功的订单id[], 删除失败的订单id及错误信息[]),比如删除三个都成功那么结果为([1xx,2xx,3xx], [])
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol.
        if len(order_nos) == 0:
            success, error = await self._rest_api.cancel_orders(symbol)
            if error:
                return False, error
            if not success["success"]:
                return False, "cancel_orders error"
            return True, None
        # If len(order_nos) > 0, you will cancel an or multiple orders.
        else:
            success, error = [], []
            for order_no in order_nos:
                _, e = await self._rest_api.cancel_order(order_no)
                if e:
                    error.append((order_no, e))
                else:
                    success.append(order_no)
            return tuple(success), tuple(error)

    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        
        #{"result": {"backstopProvider": false, "collateral": 110.094266926, "freeCollateral": 109.734306926, "initialMarginRequirement": 0.2, "leverage": 5.0, "liquidating": false, "maintenanceMarginRequirement": 0.03, "makerFee": 0.0002, "marginFraction": 61.1703338848761, "openMarginFraction": 61.170278323147016, "positionLimit": null, "positionLimitUsed": 2.15976, "positions": [{"collateralUsed": 0.35996, "cost": -1.7999, "entryPrice": 179.99, "estimatedLiquidationPrice": 11184.0172926, "future": "ETH-PERP", "initialMarginRequirement": 0.2, "longOrderSize": 0.0, "maintenanceMarginRequirement": 0.03, "netSize": -0.01, "openSize": 0.01, "realizedPnl": 0.01723393, "shortOrderSize": 0.0, "side": "sell", "size": 0.01, "unrealizedPnl": 0.0001}], "takerFee": 0.0007, "totalAccountValue": 110.094366926, "totalPositionSize": 1.7998, "useFttCollateral": true, "username": "8342537@qq.com"}, "success": true}
        
        success, error = await self._rest_api.get_account_info()
        if error:
            return None, error
        
        if not success["success"]:
            return None, "get_account_info error"
        
        data = success["result"]
        
        assets = {}
        total = float(data["collateral"])
        free = float(data["freeCollateral"])
        locked = total - free
        assets["USD"] = {
            "total": "%.8f" % total,
            "free": "%.8f" % free,
            "locked": "%.8f" % locked
        }
        if assets == self._assets:
            update = False
        else:
            update = True
        self._assets = assets
        timestamp = tools.get_cur_timestamp_ms()
        
        ast = Asset(self._platform, self._account, self._assets, timestamp, update)
        
        #因为ftx websocket接口里面没有资产通知,所以只能这样模拟
        if self.cb.on_asset_update_callback:
            SingleTask.run(self.cb.on_asset_update_callback, ast)
        
        return ast, None

    def _convert_order_format(self, o):
        """将交易所订单结构转换为本交易系统标准订单结构格式
        """
        order_no = str(o["id"])
        state = o["status"]
        remain = float(o["remainingSize"])
        filled = float(o["filledSize"])
        size = float(o["size"])
        price = None if o["price"]==None else float(o["price"])
        avg_price = None if o["avgFillPrice"]==None else float(o["avgFillPrice"])      
        if state == "new":
            status = ORDER_STATUS_SUBMITTED
        elif state == "open":
            if remain < size:
                status = ORDER_STATUS_PARTIAL_FILLED
            else:
                status = ORDER_STATUS_SUBMITTED
        elif state == "closed":
            if filled < size:
                status = ORDER_STATUS_CANCELED
            else:
                status = ORDER_STATUS_FILLED
        else:
            return None
        info = {
            "platform": self._platform,
            "account": self._account,
            "strategy": self._strategy,
            "order_no": order_no,
            "action": ORDER_ACTION_BUY if o["side"] == "buy" else ORDER_ACTION_SELL,
            "symbol": o["market"],
            "price": price,
            "quantity": size,
            "order_type": ORDER_TYPE_LIMIT if o["type"] == "limit" else ORDER_TYPE_MARKET,
            "remain": remain, #size-filled会更好
            "status": status,
            "avg_price": avg_price
        }
        order = Order(**info)
        return order

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        
        #{"result": [{"avgFillPrice": null, "clientId": null, "createdAt": null, "filledSize": 0.0, "future": "ETH-PERP", "id": 769622011, "ioc": false, "market": "ETH-PERP", "postOnly": false, "price": 152.0, "reduceOnly": false, "remainingSize": 0.002, "side": "buy", "size": 0.002, "status": "open", "type": "limit"}, {"avgFillPrice": null, "clientId": null, "createdAt": null, "filledSize": 0.0, "future": "ETH-PERP", "id": 769620713, "ioc": false, "market": "ETH-PERP", "postOnly": false, "price": 150.0, "reduceOnly": false, "remainingSize": 0.001, "side": "buy", "size": 0.001, "status": "open", "type": "limit"}], "success": true}
        
        orders:List[Order] = []
        
        success, error = await self._rest_api.get_open_orders(symbol)
        if error:
            return None, error
        
        if not success["success"]:
            return None, "get_open_orders error"
        
        data = success["result"]
        for o in data:
            order = self._convert_order_format(o)
            if order == None:
                return None, "get_open_orders error"
            orders.append(order)

        return orders, None

    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        
        #{"result": [{"collateralUsed": 0.35986, "cost": -1.7984, "entryPrice": 179.84, "estimatedLiquidationPrice": 11184.0123266, "future": "ETH-PERP", "initialMarginRequirement": 0.2, "longOrderSize": 0.0, "maintenanceMarginRequirement": 0.03, "netSize": -0.01, "openSize": 0.01, "realizedPnl": 0.01866927, "recentAverageOpenPrice": 179.84, "recentPnl": -0.0009, "shortOrderSize": 0.0, "side": "sell", "size": 0.01, "unrealizedPnl": -0.0009}], "success": true}

        success, error = await self._rest_api.get_positions(True)
        if error:
            return None, error
        if not success["success"]:
            return None, "get_position error"
        
        p = next(filter(lambda x: x['future'] == symbol, success["result"]), None)
        if p == None:
            return None, "symbol not exist"
        
        if p["netSize"] == 0:
            return None, "currently no positions"

        pos = Position(self._platform, self._account, self._strategy, symbol)
        if p["netSize"] < 0:
            #空头仓位
            pos.update(short_quantity=abs(p["netSize"]), short_avg_price=p["recentAverageOpenPrice"], liquid_price=p["estimatedLiquidationPrice"])
        else:
            #多头仓位
            pos.update(long_quantity=abs(p["netSize"]), long_avg_price=p["recentAverageOpenPrice"], liquid_price=p["estimatedLiquidationPrice"])

        #因为ftx websocket接口里面没有仓位通知,所以只能这样模拟
        if self.cb.on_position_update_callback:
            SingleTask.run(self.cb.on_position_update_callback, pos)

        return pos, None

    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        #{"result": {"ask": 150.42, "bid": 150.39, "change1h": 0.0019984012789768186, "change24h": -0.0572234409276089, "changeBod": 0.004407051282051282, "description": "Ethereum Perpetual Futures", "enabled": true, "expired": false, "expiry": null, "expiryDescription": "Perpetual", "index": 150.60309063, "last": 150.53, "lowerBound": 143.0, "mark": 150.42, "moveStart": null, "name": "ETH-PERP", "perpetual": true, "postOnly": false, "priceIncrement": 0.01, "sizeIncrement": 0.001, "type": "perpetual", "underlying": "ETH", "underlyingDescription": "Ethereum", "upperBound": 158.13, "volume": 244685.546, "volumeUsd24h": 36640126.93856}, "success": true}

        success, error = await self._rest_api.get_future(symbol)
        if error:
            return None, error
        if not success["success"]:
            return None, "get_future error"

        data = success["result"]

        syminfo = SymbolInfo(self._platform, symbol, price_tick=data["priceIncrement"], size_tick=data["sizeIncrement"])

        return syminfo, None

    async def _login(self):
        """FTX的websocket接口真是逗逼,验证成功的情况下居然不会返回任何消息"""
        ts = int(time.time() * 1000)
        signature = hmac.new(self._secret_key.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest()
        args = {
            'key': self._access_key,
            'sign': signature,
            'time': ts
        }
        #如果是子账户,就添加相应字段
        if self._subaccount_name:
            args["subaccount"] = self._subaccount_name
        data = {'op': 'login', 'args': args}
        await self.ws.send_json(data)

    async def _kline_loop_query(self, symbol, *args, **kwargs):
        #{"result": [{"close": 7088.5, "high": 7090.0, "low": 7085.75, "open": 7090.0, "startTime": "2019-11-26T16:44:00+00:00", "time": 1574786640000.0, "volume": 0.70885}, {"close": 7088.0, "high": 7088.75, "low": 7088.0, "open": 7088.5, "startTime": "2019-11-26T16:45:00+00:00", "time": 1574786700000.0, "volume": 0.708875}], "success": true}
        success, error = await self._rest_api.get_kline(symbol, 60, 2) #取2个时间窗口的数据
        if error:
            return None, error
        if not success["success"]:
            return None, "_kline_loop_query error"
        result = success["result"]
        k = result[0] #这里保存的是上一分钟完整的数据
        self._update_kline(k, symbol)

    async def connected_callback(self):
        """网络链接成功回调
        """
        if self._account != None:
            #账号不为空就要进行登录认证,然后订阅2个需要登录后才能订阅的私有频道:用户挂单通知和挂单成交通知(FTX只支持这2个私有频道)
            await self._login() #登录认证
            #`用户挂单通知回调`不为空,就进行订阅
            if self.cb.on_order_update_callback != None:
                await self.ws.send_json({'op': 'subscribe', 'channel': 'orders'})
            #`用户挂单成交通知回调`不为空,就进行订阅
            if self.cb.on_fill_update_callback != None:
                await self.ws.send_json({'op': 'subscribe', 'channel': 'fills'})

        #订阅公共频道,无需登录认证
        for sym in self._symbols:
            if self.cb.on_trade_update_callback != None:
                await self.ws.send_json({'op': 'subscribe', 'channel': 'trades', 'market': sym})

            if self.cb.on_orderbook_update_callback != None:
                await self.ws.send_json({'op': 'subscribe', 'channel': 'orderbook', 'market': sym})

            if self.cb.on_ticker_update_callback != None:
                await self.ws.send_json({'op': 'subscribe', 'channel': 'ticker', 'market': sym})

            if self.cb.on_kline_update_callback != None:
                LoopRunTask.register(self._kline_loop_query, 60, sym)

    async def process(self, msg):
        """ Process message that received from websocket.

        Args:
            msg: message received from websocket.

        Returns:
            None.
        """
        
        if not isinstance(msg, dict):
            return
        logger.debug("msg:", json.dumps(msg), caller=self)
        
        #{"type": "error", "code": 400, "msg": "Invalid login credentials"}
        if msg["type"] == "error":
            e = Error("Websocket connection failed: {}".format(msg))
            logger.error(e, caller=self)
            SingleTask.run(self.cb.on_init_success_callback, False, e, **self.raw_kwargs)
            return
        
        if msg["type"] == "info" and msg["code"] == 20001:
            #交易所重启了,我们就断开连接,websocket会自动重连
            @async_method_locker("FTXTrader._ws_close.locker")
            async def _ws_close(self):
                await self.ws.close()
            SingleTask.run(self._ws_close)
            return
        
        #{'type': 'subscribed', 'channel': 'trades', 'market': 'BTC-PERP'}
        if msg["type"] == "unsubscribed":
            return
        
        if msg["type"] == "subscribed":
            for sym in self._symbols:
                if self.cb.on_order_update_callback != None:
                    orders, err = await self.get_orders(sym)
                    if not err:
                        for o in orders:
                            SingleTask.run(self.cb.on_order_update_callback, o)
            #end for
            return

        channel = msg['channel']
        if channel == 'orderbook':
            self._update_orderbook(msg)
        elif channel == 'trades':
            self._update_trades(msg)
        elif channel == 'ticker':
            self._update_ticker(msg)
        elif channel == 'orders':
            self._update_order(msg)
        elif channel == 'fills':
            self._update_fill(msg)

    def _update_ticker(self, ticker_info):
        """ ticker update.

        Args:
            ticker_info: ticker information.

        Returns:
        """
        #{"channel": "ticker", "market": "BTC-PERP", "type": "update", "data": {"bid": 9320.0, "ask": 9323.0, "bidSize": 78.506, "askSize": 101.2467, "last": 9333.5, "time": 1573014477.9969265}}
        ts = int(float(ticker_info["data"]["time"])*1000) #转变为毫秒
        p = {
            "platform": self._platform,
            "symbol": ticker_info["market"],
            "ask": ticker_info["data"]["ask"],
            "bid": ticker_info["data"]["bid"],
            "last": ticker_info["data"]["last"],
            "timestamp": ts
        }
        ticker = Ticker(**p)
        SingleTask.run(self.cb.on_ticker_update_callback, ticker)

    def _update_trades(self, trades_info):
        """ trades update.

        Args:
            trades_info: trades information.

        Returns:
        """
        #{"channel": "trades", "market": "BTC-PERP", "type": "update", "data": [{"id": 2616562, "price": 9333.25, "size": 0.2143, "side": "sell", "liquidation": false, "time": "2019-11-06T05:19:51.187372+00:00"}]} 
        for t in trades_info["data"]:
            ts = tools.utctime_str_to_mts(t["time"], "%Y-%m-%dT%H:%M:%S.%f+00:00")
            p = {
                "platform": self._platform,
                "symbol": trades_info["market"],
                "action": ORDER_ACTION_BUY if t["side"] == "buy" else ORDER_ACTION_SELL,
                "price": t["price"],
                "quantity": t["size"],
                "timestamp": ts
            }
            trade = Trade(**p)
            SingleTask.run(self.cb.on_trade_update_callback, trade)
    
    def _reset_orderbook(self, market: str) -> None:
        if market in self._orderbooks:
            del self._orderbooks[market]
    
    def _get_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        return {
            side: sorted(
                [(price, quantity) for price, quantity in list(self._orderbooks[market][side].items()) if quantity],
                key=lambda order: order[0] * (-1 if side == 'bids' else 1)
            )
            for side in {'bids', 'asks'}
        }

    def _update_orderbook(self, orderbook_info):
        """ orderbook update.

        Args:
            orderbook_info: orderbook information.

        Returns:
        """
        market = orderbook_info['market']
        data = orderbook_info['data']
        if data['action'] == 'partial':
            self._reset_orderbook(market)
        for side in {'bids', 'asks'}:
            book = self._orderbooks[market][side]
            for price, size in data[side]:
                if size:
                    book[price] = size
                else:
                    del book[price]
        #end for
        checksum = data['checksum']
        orderbook = self._get_orderbook(market)
        checksum_data = [
            ':'.join([f'{float(order[0])}:{float(order[1])}' for order in (bid, offer) if order])
            for (bid, offer) in zip_longest(orderbook['bids'][:100], orderbook['asks'][:100])
        ]
        computed_result = int(zlib.crc32(':'.join(checksum_data).encode()))
        if computed_result != checksum:
            #校验和不对就需要重新订阅深度信息
            @async_method_locker("FTXTrader._re_subscribe.locker")
            async def _re_subscribe(self):
                await self.ws.send_json({'op': 'unsubscribe', 'channel': 'orderbook', 'market': market})
                await self.ws.send_json({'op': 'subscribe', 'channel': 'orderbook', 'market': market})
            SingleTask.run(self._re_subscribe)
            #校验和不对就退出
            return
        
        logger.debug("orderbook:", json.dumps(orderbook), caller=self)

        ts = int(float(data['time'])*1000) #转变为毫秒
        p = {
            "platform": self._platform,
            "symbol": market,
            "asks": orderbook['asks'],
            "bids": orderbook['bids'],
            "timestamp": ts
        }
        ob = Orderbook(**p)
        SingleTask.run(self.cb.on_orderbook_update_callback, ob)

    def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.

        Returns:
            None.
        """
        #new (accepted but not processed yet), open, or closed (filled or cancelled)

        #开仓
        #{"id": 742849571, "clientId": null, "market": "ETH-PERP", "type": "limit", "side": "buy", "price": 150.0, "size": 0.003, "status": "new", "filledSize": 0.0, "remainingSize": 0.003, "reduceOnly": false, "avgFillPrice": null, "postOnly": false, "ioc": false}
        
        #150->修改->151
        #{"id": 742849571, "clientId": null, "market": "ETH-PERP", "type": "limit", "side": "buy", "price": 150.0, "size": 0.003, "status": "closed", "filledSize": 0.0, "remainingSize": 0.0, "reduceOnly": false, "avgFillPrice": null, "postOnly": false, "ioc": false}
        #{"id": 742853455, "clientId": null, "market": "ETH-PERP", "type": "limit", "side": "buy", "price": 151.0, "size": 0.003, "status": "new", "filledSize": 0.0, "remainingSize": 0.003, "reduceOnly": false, "avgFillPrice": null, "postOnly": false, "ioc": false}
        
        #151->修改->187->成交
        #{"id": 742853455, "clientId": null, "market": "ETH-PERP", "type": "limit", "side": "buy", "price": 151.0, "size": 0.003, "status": "closed", "filledSize": 0.0, "remainingSize": 0.0, "reduceOnly": false, "avgFillPrice": null, "postOnly": false, "ioc": false}
        #{"id": 742862380, "clientId": null, "market": "ETH-PERP", "type": "limit", "side": "buy", "price": 187.0, "size": 0.003, "status": "closed", "filledSize": 0.003, "remainingSize": 0.0, "reduceOnly": false, "avgFillPrice": 186.96, "postOnly": false, "ioc": false}
        
        #市价全平仓位
        #{"id": 742875876, "clientId": null, "market": "ETH-PERP", "type": "market", "side": "sell", "price": null, "size": 0.003, "status": "closed", "filledSize": 0.003, "remainingSize": 0.0, "reduceOnly": true, "avgFillPrice": 186.79, "postOnly": false, "ioc": true}
        
        o = order_info["data"]
        
        order = self._convert_order_format(o)
        if order == None:
            return

        SingleTask.run(self.cb.on_order_update_callback, order)

    def _update_fill(self, fill_info):
        """ Fill update.

        Args:
            fill_info: Fill information.

        Returns:
            None.
        """       
        #{"channel": "orders", "type": "update", "data": {"id": 751733812, "clientId": null, "market": "ETH-PERP", "type": "limit", "side": "buy", "price": 187.93, "size": 0.001, "status": "closed", "filledSize": 0.001, "remainingSize": 0.0, "reduceOnly": false, "avgFillPrice": 184.25, "postOnly": false, "ioc": false}} 
        #{"channel": "fills", "type": "update", "data": {"id": 5741311, "market": "ETH-PERP", "future": "ETH-PERP", "baseCurrency": null, "quoteCurrency": null, "type": "order", "side": "buy", "price": 184.25, "size": 0.001, "orderId": 751733812, "time": "2019-11-08T09:52:27.366467+00:00", "feeRate": 0.0007, "fee": 0.000128975, "liquidity": "taker"}} 

        data = fill_info["data"]
        
        fill_no = str(data["id"])
        order_no = str(data["orderId"])
        price = float(data["price"])
        size = float(data["size"])
        fee = float(data["fee"])
        ts = tools.utctime_str_to_mts(data["time"], "%Y-%m-%dT%H:%M:%S.%f+00:00")
        liquidity = LIQUIDITY_TYPE_TAKER if data["liquidity"]=="taker" else LIQUIDITY_TYPE_MAKER
        
        info = {
            "platform": self._platform,
            "account": self._account,
            "strategy": self._strategy,
            "fill_no": fill_no,
            "order_no": order_no,
            "side": ORDER_ACTION_BUY if data["side"] == "buy" else ORDER_ACTION_SELL,
            "symbol": data["market"],
            "price": price,
            "quantity": size,
            "liquidity": liquidity,
            "fee": fee,
            "ctime": ts
        }
        fill = Fill(**info)
        SingleTask.run(self.cb.on_fill_update_callback, fill)

    def _update_kline(self, kline_info, symbol):
        """ kline update.

        Args:
            kline_info: kline information.

        Returns:
            None.
        """
        info = {
            "platform": self._platform,
            "symbol": symbol,
            "open": kline_info["open"],
            "high": kline_info["high"],
            "low": kline_info["low"],
            "close": kline_info["close"],
            "volume": kline_info["volume"],
            "timestamp": tools.utctime_str_to_mts(kline_info["startTime"], "%Y-%m-%dT%H:%M:%S+00:00"),
            "kline_type": MARKET_TYPE_KLINE
        }
        kline = Kline(**info)
        SingleTask.run(self.cb.on_kline_update_callback, kline)
