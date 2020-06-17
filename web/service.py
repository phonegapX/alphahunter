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
from quant.tasks import SingleTask
from quant.state import State
from quant import const

"""
交易流: client <-----1-----> webservice <-----2-----> alphahunter量化交易接口

(socket+token+trader)三元组代表一个完整的交易通道,1由socket+token表示,2由trader表示
而每个trader都是绑定了不同的账号(account),程序中已经做了判断,account不允许重复登录,
account的定义是:access_key@platform,比如:"1b069d65-1d36ff43-afwo04df3f-6554f@huobi"
access_key和platform都是由client传上来的.
最后总结:(socket+token+trader)<-绑定->account.
因为同一个access_key可以访问同系列的接口,比如火币的access_key可以同时访问火币现货接口和火币期货接口,
所以account合理的定义为:access_key@platform,唯一代表一个登录账号,以此来判断是否会有重复登录

程序设计说明:
每个客户端都可以通过一个websocket连接上webservice,客户端可以通过这个websocket连接同时登录上多个量化交易接口进行操作,比如说当前
有两个客户端A和B,都连上了webservice,客户端A登录了火币现货,火币期货,okex现货等3个量化交易接口,客户端B登录了火币现货,FTX等2个量化交易接口,
那么这个时候我们系统里面就包括了2个websocket连接,暂时称为ws-A,ws-B, 5个交易通道分别为:火币现货A,火币期货A,okex现货A,火币现货B,FTX-B.
其中火币现货A,火币期货A,okex现货A等三个交易通道基于套接字ws-A进行通讯, 火币现货B,FTX-B等两个交易通道基于套接字ws-B进行通讯.

从token角度看,因为每个token是全局唯一的,所以可以以token作为key来保存交易通道,全局字典app._my_trader_dict就是做这个的.
从account角度来看,因为不允许重复登录,所以每个account也是全局唯一的,所以也可以以account作为key来保存交易通道,全局字典CB.ws_dict就是做这个的.
从socket角度来看,一个socket连接上面可以同时登录多个交易接口,字典resp._my_own_token_dict包含了本套接字上所有登录的接口表示.
最后就是全局变量app['sockets']保存了整个系统连接上来的所有websocket套接字

当用户通过logout退出登录某个接口的时候,需要将相应接口shutdown,然后需要将app._my_trader_dict,CB.ws_dict中的相应条目删除,然后看看这个登录是基于哪个套接字,找到
相应套接字,从resp._my_own_token_dict中删除相应条目.

当某个套接字被关闭,需要将相应resp._my_own_token_dict中的所有接口shutdown,然后把对应于app._my_trader_dict,CB.ws_dict中的相应条目全部删除,然后
把被关闭的套接字从app['sockets']中删除.具体请看cleanup函数
"""


WS_FILE = os.path.join(os.path.dirname(__file__), 'client-test.html') #网页端通过javascript测试API


routes = web.RouteTableDef() #web请求路由表
app = web.Application() #web应用程序
app._my_trader_dict = defaultdict(lambda:defaultdict(lambda:None)) #全局字典 {"token1":{"trader":交易对象1,"account":"登录账户1"},"token2":{"trader":交易对象2,"account":"登录账户2"},...}


class CB(ExchangeGateway.ICallBack):
    ws_dict = defaultdict(lambda:defaultdict(lambda:None)) #全局字典 {"登录账户1":{"socket":ws1,"token":"登录令牌1"},"登录账户2":{"socket":ws2,"token":"登录令牌2"},...}
    
    async def on_kline_update_callback(self, kline: Kline): pass
    async def on_orderbook_update_callback(self, orderbook: Orderbook): pass
    async def on_trade_update_callback(self, trade: Trade): pass
    async def on_ticker_update_callback(self, ticker: Ticker): pass
    
    def find_ws(self, account):
        """通过登录账号找到对应的通讯ws和登录令牌token.
        可以理解为socket+token组合可以唯一代表一个与客户端的交易通讯通道
        """
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
            await response(ws, r) #通知客户端
    
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
            await response(ws, r) #通知客户端
    
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
            await response(ws, r) #通知客户端
    
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
            await response(ws, r) #通知客户端
    
    async def on_state_update_callback(self, state: State, **kwargs):
        ws, token = self.find_ws(state.account)
        if ws != None and token:
            r = {
                "op": "state",
                "token": token,
                "code": state.code,
                "msg": state.msg
            }
            await response(ws, r) #通知客户端


app._my_callback = CB()


async def response(socket, msg):
    await socket.send_json(msg)


def find_trader(token):
    """通过登录令牌找到相应的trader
    """
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
    kwargs["account"] = msg["access_key"] + "@" + msg["platform"]
    kwargs["access_key"] = msg["access_key"]
    kwargs["secret_key"] = msg["secret_key"]
    k = kwargs["account"]
    token = tools.get_uuid5(k+str(tools.get_cur_timestamp_ms())) #生成一个全局唯一token,代表一个交易通道
    if cb.ws_dict[k]["token"]: #不允许重复登录
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
                        "quote_currency": success.quote_currency,
                        "settlement_currency": success.settlement_currency
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
        trader.shutdown() #关闭接口
        k = app._my_trader_dict[token]["account"]
        if k:
            cb = app._my_callback
            del cb.ws_dict[k] #删除相应条目
        del app._my_trader_dict[token] #删除相应条目
        del resp._my_own_token_dict[token] #删除相应条目
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
    for token in ws._my_own_token_dict: #将基于本套接字的所有交易通道全部关闭
        trader = find_trader(token)
        if trader:
            trader.shutdown() #关闭接口
        k = app._my_trader_dict[token]["account"]
        if k:
            cb = app._my_callback
            del cb.ws_dict[k] #删除相应条目
        del app._my_trader_dict[token] #删除相应条目
    del ws._my_own_token_dict #这行代码其实可以不需要
    app['sockets'].remove(ws) #最后从列表中移除即将关闭的套接字


@routes.get('/ws/v1')
async def wshandler(request):
    resp = web.WebSocketResponse()
    available = resp.can_prepare(request)
    if not available:
        with open(WS_FILE, 'rb') as fp:
            return web.Response(body=fp.read(), content_type='text/html')

    await resp.prepare(request)
    
    resp._my_own_token_dict = {} #用于保存基于本套接字的所有交易通道
    
    resp._my_last_ts = tools.get_cur_timestamp_ms()
    request.app['sockets'].append(resp) #用于判断套接字是否还是活跃状态

    async for msg in resp: #开始接收消息
        if msg.type == web.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                await process(resp, data) #处理消息
                resp._my_last_ts = tools.get_cur_timestamp_ms() #保存最后活跃时间,用于判断套接字是否还是活跃状态
            except Exception as e:
                logger.error("process ERROR:", e)
                r = {
                    "op": "error",
                    "error_message": str(e)
                }
                SingleTask.run(response, resp, r) #产生错误,发送错误信息
    cleanup(resp) #清理此套接字下的所有交易通道
    return resp


async def on_time():
    """检测是否有套接字已经不活跃
    """
    ts = tools.get_cur_timestamp_ms()
    for ws in app['sockets']:
        if ts - ws._my_last_ts > 15*1000: #超过15秒
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
    web.run_app(app, port=9878)