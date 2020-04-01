# -*- coding:utf-8 -*-

"""
量化交易API服务

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import os
import textwrap
import json
import copy
from collections import defaultdict
from aiohttp import web

from quant.utils import tools, logger
from quant.gateway import ExchangeGateway
from quant.market import Kline, Orderbook, Trade, Ticker
from quant.asset import Asset
from quant.position import Position
from quant.order import Order, Fill, ORDER_TYPE_LIMIT
from quant.trader import Trader
from quant.tasks import LoopRunTask, SingleTask
from quant.state import State
from quant import const


WS_FILE = os.path.join(os.path.dirname(__file__), 'client-test.html')


routes = web.RouteTableDef()
app = web.Application()
app._my_trader_dict = defaultdict(lambda:defaultdict(lambda:None))


class CB(ExchangeGateway.ICallBack):
    ws_dict = defaultdict(lambda:defaultdict(lambda:None))
    
    async def on_kline_update_callback(self, kline: Kline): pass
    async def on_orderbook_update_callback(self, orderbook: Orderbook): pass
    async def on_trade_update_callback(self, trade: Trade): pass
    async def on_ticker_update_callback(self, ticker: Ticker): pass
    
    def find_ws(self, account):
        ws = self.ws_dict[account]["socket"]
        token = self.ws_dict[account]["token"]
        return ws, token
    
    async def on_asset_update_callback(self, asset: Asset):
        ws, token = self.find_ws(asset.account)
        if ws != None and token:
            r = {
                "op": "notify",
                "topic": "asset",
                "token": token,
                "data": asset.assets
            }
            await response(ws, r)
    
    async def on_position_update_callback(self, position: Position):
        ws, token = self.find_ws(position.account)
        if ws != None and token:
            r = {
                "op": "notify",
                "topic": "position",
                "token": token,
                "data": {
                    "symbol": position.symbol,
                    
                    "margin_mode": position.margin_mode,
        
                    "long_quantity": position.long_quantity,
                    "long_avail_qty": position.long_avail_qty,
                    "long_open_price": position.long_open_price,
                    "long_hold_price": position.long_hold_price,
                    "long_liquid_price": position.long_liquid_price,
                    "long_unrealised_pnl": position.long_unrealised_pnl,
                    "long_leverage": position.long_leverage,
                    "long_margin": position.long_margin,
        
                    "short_quantity": position.short_quantity,
                    "short_avail_qty": position.short_avail_qty,
                    "short_open_price": position.short_open_price,
                    "short_hold_price": position.short_hold_price,
                    "short_liquid_price": position.short_liquid_price,
                    "short_unrealised_pnl": position.short_unrealised_pnl,
                    "short_leverage": position.short_leverage,
                    "short_margin": position.short_margin,
        
                    "utime": position.utime
                }
            }
            await response(ws, r)
    
    async def on_order_update_callback(self, order: Order):
        ws, token = self.find_ws(order.account)
        if ws != None and token:
            r = {
                "op": "notify",
                "topic": "order",
                "token": token,
                "data": {
                    "symbol": order.symbol,
                    "order_no": order.order_no,
                    "action": order.action,
                    "price": order.price,
                    "quantity": order.quantity,
                    "remain": order.remain,
                    "status": order.status,
                    "order_type": order.order_type,
                    "ctime": order.ctime,
                    "utime": order.utime
                }
            }
            await response(ws, r)
    
    async def on_fill_update_callback(self, fill: Fill):
        ws, token = self.find_ws(fill.account)
        if ws != None and token:
            r = {
                "op": "notify",
                "topic": "fill",
                "token": token,
                "data": {
                    "symbol": fill.symbol,
                    "order_no": fill.order_no,
                    "fill_no": fill.fill_no,
                    "price": fill.price,
                    "quantity": fill.quantity,
                    "side": fill.side,
                    "liquidity": fill.liquidity,
                    "ctime": fill.ctime
                }
            }
            await response(ws, r)
    
    async def on_state_update_callback(self, state: State, **kwargs):
        ws, token = self.find_ws(state.account)
        if ws != None and token:
            r = {
                "op": "state",
                "token": token,
                "code": state.code,
                "msg": state.msg
            }
            await response(ws, r)


app._my_callback = CB()


async def response(socket, msg):
    await socket.send_json(msg)


def find_trader(token):
    trader = None
    if token in app._my_trader_dict:
        trader = app._my_trader_dict[token]["trader"]
    return trader


def login(resp, msg):
    cb = app._my_callback
    cb.on_kline_update_callback = None
    cb.on_orderbook_update_callback = None
    cb.on_trade_update_callback = None
    cb.on_ticker_update_callback = None
    kwargs = {}
    kwargs["cb"] = cb
    kwargs["strategy"] = "__webservice__"
    kwargs["platform"] = msg["platform"]
    kwargs["symbols"] = msg["symbols"]
    kwargs["account"] = msg["access_key"] + "@" + msg["account"] + "@" + msg["platform"]
    kwargs["access_key"] = msg["access_key"]
    kwargs["secret_key"] = msg["secret_key"]
    k = kwargs["account"]
    token = tools.get_uuid5(k+str(tools.get_cur_timestamp_ms()))
    if cb.ws_dict[k]["token"]:
        r = {
            "op": "login",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Do not log in repeat",
            "token": ""
        }
        SingleTask.run(response, resp, r)
        return
    app._my_trader_dict[token]["trader"] = Trader(**kwargs)
    app._my_trader_dict[token]["account"] = k
    cb.ws_dict[k]["socket"] = resp
    cb.ws_dict[k]["token"] = token
    resp._my_own_token_dict[token] = True
    r = {
        "op": "login",
        "cid": msg.get("cid", ""),
        "result": True,
        "error_message": "",
        "token": token
    }
    SingleTask.run(response, resp, r)


def place_order(resp, msg):
    token = msg["token"]
    trader = find_trader(token)
    if trader:        
        kwargs = {}
        kwargs["symbol"] = msg["symbol"]
        kwargs["action"] = msg["action"]
        kwargs["price"] = msg["price"]
        kwargs["quantity"] = msg["quantity"]
        kwargs["order_type"] = msg["order_type"]
        
        async def _work(trader, kwargs):
            order_no, error = await trader.create_order(**kwargs)
            if error:
                r = {
                    "op": "place_order",
                    "cid": msg.get("cid", ""),
                    "result": False,
                    "error_message": str(error)
                }
            else:
                r = {
                    "op": "place_order",
                    "cid": msg.get("cid", ""),
                    "result": True,
                    "error_message": "",
                    "order_no": order_no
                }
            await response(resp, r)

        SingleTask.run(_work, trader, kwargs)
    else:
        r = {
            "op": "place_order",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Invalid token"
        }
        SingleTask.run(response, resp, r)


def cancel_order(resp, msg):
    token = msg["token"]
    trader = find_trader(token)
    if trader:
        kwargs = {}
        kwargs["symbol"] = msg["symbol"]
        kwargs["order_nos"] = msg["order_nos"]

        async def _work(trader, kwargs):
            symbol = kwargs["symbol"]
            order_nos = kwargs["order_nos"]
            success, error = await trader.revoke_order(symbol, *order_nos)
            if error:
                r = {
                    "op": "cancel_order",
                    "cid": msg.get("cid", ""),
                    "result": False,
                    "error_message": str(error)
                }
            else:
                data = []
                for o in success:
                    s = o[0]
                    e = o[1]
                    if e:
                        r = {
                            "result": False,
                            "error_message": str(e),
                            "order_no": s
                        }
                    else:
                        r = {
                            "result": True,
                            "error_message": "",
                            "order_no": s
                        }
                    data.append(r)
                r = {
                    "op": "cancel_order",
                    "cid": msg.get("cid", ""),
                    "result": True,
                    "error_message": "",
                    "data": data
                }
            await response(resp, r)

        SingleTask.run(_work, trader, kwargs)
    else:
        r = {
            "op": "cancel_order",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Invalid token"
        }
        SingleTask.run(response, resp, r)


def open_orders(resp, msg):
    token = msg["token"]
    trader = find_trader(token)
    if trader:
        kwargs = {}
        kwargs["symbol"] = msg["symbol"]

        async def _work(trader, kwargs):
            symbol = kwargs["symbol"]
            success, error = await trader.get_orders(symbol)
            if error:
                r = {
                    "op": "open_orders",
                    "cid": msg.get("cid", ""),
                    "result": False,
                    "error_message": str(error)
                }
            else:
                data = []
                for o in success:
                    r = {
                            "order_no": o.order_no,
                            "action": o.action,
                            "price": o.price,
                            "quantity": o.quantity,
                            "remain": o.remain,
                            "status": o.status,
                            "order_type": o.order_type,
                            "ctime": o.ctime,
                            "utime": o.utime
                    }
                    data.append(r)
                r = {
                    "op": "open_orders",
                    "cid": msg.get("cid", ""),
                    "result": True,
                    "error_message": "",
                    "data": data
                }
            await response(resp, r)

        SingleTask.run(_work, trader, kwargs)
    else:
        r = {
            "op": "open_orders",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Invalid token"
        }
        SingleTask.run(response, resp, r)


def asset(resp, msg):
    token = msg["token"]
    trader = find_trader(token)
    if trader:

        async def _work(trader):
            success, error = await trader.get_assets()
            if error:
                r = {
                    "op": "asset",
                    "cid": msg.get("cid", ""),
                    "result": False,
                    "error_message": str(error)
                }
            else:
                r = {
                    "op": "asset",
                    "cid": msg.get("cid", ""),
                    "result": True,
                    "error_message": "",
                    "data": success.assets
                }
            await response(resp, r)

        SingleTask.run(_work, trader)
    else:
        r = {
            "op": "asset",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Invalid token"
        }
        SingleTask.run(response, resp, r)


def position(resp, msg):
    token = msg["token"]
    trader = find_trader(token)
    if trader:
        kwargs = {}
        kwargs["symbol"] = msg["symbol"]

        async def _work(trader, kwargs):
            symbol = kwargs["symbol"]
            success, error = await trader.get_position(symbol)
            if error:
                r = {
                    "op": "position",
                    "cid": msg.get("cid", ""),
                    "result": False,
                    "error_message": str(error)
                }
            else:
                r = {
                    "op": "position",
                    "cid": msg.get("cid", ""),
                    "result": True,
                    "error_message": "",
                    "data": {
                        "margin_mode": success.margin_mode,
            
                        "long_quantity": success.long_quantity,
                        "long_avail_qty": success.long_avail_qty,
                        "long_open_price": success.long_open_price,
                        "long_hold_price": success.long_hold_price,
                        "long_liquid_price": success.long_liquid_price,
                        "long_unrealised_pnl": success.long_unrealised_pnl,
                        "long_leverage": success.long_leverage,
                        "long_margin": success.long_margin,
            
                        "short_quantity": success.short_quantity,
                        "short_avail_qty": success.short_avail_qty,
                        "short_open_price": success.short_open_price,
                        "short_hold_price": success.short_hold_price,
                        "short_liquid_price": success.short_liquid_price,
                        "short_unrealised_pnl": success.short_unrealised_pnl,
                        "short_leverage": success.short_leverage,
                        "short_margin": success.short_margin,

                        "utime": success.utime
                    }
                }
            await response(resp, r)

        SingleTask.run(_work, trader, kwargs)
    else:
        r = {
            "op": "position",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Invalid token"
        }
        SingleTask.run(response, resp, r)


def symbol_info(resp, msg):
    token = msg["token"]
    trader = find_trader(token)
    if trader:
        kwargs = {}
        kwargs["symbol"] = msg["symbol"]

        async def _work(trader, kwargs):
            symbol = kwargs["symbol"]
            success, error = await trader.get_symbol_info(symbol)
            if error:
                r = {
                    "op": "symbol_info",
                    "cid": msg.get("cid", ""),
                    "result": False,
                    "error_message": str(error)
                }
            else:
                r = {
                    "op": "symbol_info",
                    "cid": msg.get("cid", ""),
                    "result": True,
                    "error_message": "",
                    "data": {
                        "price_tick": success.price_tick,
                        "size_tick": success.size_tick,
                        "size_limit": success.size_limit,
                        "value_tick": success.value_tick,
                        "value_limit": success.value_limit,
                        "base_currency": success.base_currency,
                        "quote_currency": success.quote_currency
                    }
                }
            await response(resp, r)

        SingleTask.run(_work, trader, kwargs)
    else:
        r = {
            "op": "symbol_info",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Invalid token"
        }
        SingleTask.run(response, resp, r)


def logout(resp, msg):
    token = msg["token"]
    trader = find_trader(token)
    if trader:
        trader.shutdown()
        k = app._my_trader_dict[token]["account"]
        if k:
            cb = app._my_callback
            del cb.ws_dict[k]
        del app._my_trader_dict[token]
        del resp._my_own_token_dict[token]
        r = {
            "op": "logout",
            "cid": msg.get("cid", ""),
            "result": True,
            "error_message": ""
        }
        SingleTask.run(response, resp, r)
    else:
        r = {
            "op": "logout",
            "cid": msg.get("cid", ""),
            "result": False,
            "error_message": "Invalid token"
        }
        SingleTask.run(response, resp, r)


async def process(resp, msg):
    """ 处理websocket上接收到的消息 text 类型
    """
    op = msg["op"]
    if op == "ping":
        r = {
            "op": "pong",
            "ts": msg["ts"]
        }
        SingleTask.run(response, resp, r)
    if op == "login":  #登录授权
        login(resp, msg)
    elif op == "place_order":  #下单
        place_order(resp, msg)
    elif op == "cancel_order":  #撤销订单
        cancel_order(resp, msg)
    elif op == "open_orders":  #查询当前未成交订单
        open_orders(resp, msg)
    elif op == "asset":   #查询账户资产
        asset(resp, msg)
    elif op == "position":   #查询当前持仓
        position(resp, msg)
    elif op == "symbol_info":   #查询符号信息
        symbol_info(resp, msg)
    elif op == "logout":   #退出登录
        logout(resp, msg)


def cleanup(ws):
    for token in ws._my_own_token_dict:
        trader = find_trader(token)
        if trader:
            trader.shutdown()
        k = app._my_trader_dict[token]["account"]
        if k:
            cb = app._my_callback
            del cb.ws_dict[k]
        del app._my_trader_dict[token]
    del ws._my_own_token_dict
    app['sockets'].remove(ws)


@routes.get('/ws/v1')
async def wshandler(request):
    resp = web.WebSocketResponse()
    available = resp.can_prepare(request)
    if not available:
        with open(WS_FILE, 'rb') as fp:
            return web.Response(body=fp.read(), content_type='text/html')

    await resp.prepare(request)
    
    resp._my_own_token_dict = {}
    
    resp._my_last_ts = tools.get_cur_timestamp_ms()
    request.app['sockets'].append(resp)

    async for msg in resp:
        if msg.type == web.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                await process(resp, data)
                resp._my_last_ts = tools.get_cur_timestamp_ms()
            except Exception as e:
                logger.error("process ERROR:", e)
                r = {
                    "op": "error",
                    "error_message": str(e)
                }
                SingleTask.run(response, resp, r)
    cleanup(resp)
    return resp


async def on_time():
    ts = tools.get_cur_timestamp_ms()
    sockets = copy.copy(app['sockets'])
    for ws in sockets:
        if ts - ws._my_last_ts > 15*1000:
            await ws.close()


async def _later_call():
    """延时调用
    """
    await on_time()
    SingleTask.call_later(_later_call, 16)


async def on_shutdown(app):
    for ws in app['sockets']:
        await ws.close()


if __name__ == '__main__':
    app['sockets'] = []
    app.router.add_routes(routes)
    app.on_shutdown.append(on_shutdown)
    SingleTask.call_later(_later_call, 16)
    web.run_app(app)