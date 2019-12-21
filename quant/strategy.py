# -*- coding:utf-8 -*-

"""
strategy template.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import asyncio
from collections import namedtuple

from quant.gateway import ExchangeGateway
from quant.market import Kline, Orderbook, Trade, Ticker, Market
from quant.asset import Asset
from quant.position import Position
from quant.order import Order, Fill, ORDER_TYPE_LIMIT
from quant.portfoliomanager import PortfolioManager
from quant.trader import Trader
from quant.tasks import LoopRunTask, SingleTask
from quant.error import Error
from quant import const


class Strategy(ExchangeGateway.ICallBack):
    """策略模板类,各种策略的基类,要实现策略需要从这个类派生出子类
    """

    TOrder = namedtuple("TOrder", ["gateway", "symbol", "action", "price", "quantity", "order_type"])
    TOrder.__new__.__defaults__ = (ORDER_TYPE_LIMIT,) #设置最后一个字段的默认值

    def __init__(self):
        """ 初始化
        """
        self._interval = 0
        self._pm = PortfolioManager()
        self._original_on_asset_update_callback = None
        self._original_on_position_update_callback = None
        self._original_on_order_update_callback = None
        self._original_on_fill_update_callback = None
        self._hook_strategy()
    
    def _hook_strategy(self):
        """Hook策略相应账户各种私有数据的回调通知函数,这样策略执行后,资产,仓位,订单,成交等数据发生变化时,
        可以第一时间感知到,并且进行相应的预处理,比如记录到数据管理器中,比如发布到消息队列服务器等 
        """
        #Hook资产回调函数
        self._original_on_asset_update_callback = self.on_asset_update_callback
        self.on_asset_update_callback = self._on_asset_update_hook
        #Hook仓位回调函数
        self._original_on_position_update_callback = self.on_position_update_callback
        self.on_position_update_callback = self._on_position_update_hook
        #Hook订单回调函数
        self._original_on_order_update_callback = self.on_order_update_callback
        self.on_order_update_callback = self._on_order_update_hook
        #Hook成交回调函数
        self._original_on_fill_update_callback = self.on_fill_update_callback
        self.on_fill_update_callback = self._on_fill_update_hook
    
    async def _on_asset_update_hook(self, asset: Asset):
        """资产变化预处理
        """
        #这里还可以将数据发布到自己搭建的消息队列服务器上去.这样其他程序,比如数据库记录程序可以从消息队列服务器订阅相对应的数据,
        #并且记录到数据库里面,以后可以按策略分类进行离线分析,生成报表等等功能,这样交易执行程序和策略研究,报表生成等程序之间可以解耦合
        #================
        self._pm.on_asset_update(asset)
        await self._original_on_asset_update_callback(asset)

    async def _on_position_update_hook(self, position: Position):
        """仓位变化预处理
        """
        self._pm.on_position_update(position)
        await self._original_on_position_update_callback(position)

    async def _on_order_update_hook(self, order: Order):
        """订单变化预处理
        """
        self._pm.on_order_update(order)
        await self._original_on_order_update_callback(order)

    async def _on_fill_update_hook(self, fill: Fill):
        """订单成交预处理
        """
        #这里还可以将数据发布到自己搭建的消息队列服务器上去.这样其他程序,比如数据库记录程序可以从消息队列服务器订阅相对应的数据,
        #并且记录到数据库里面,以后可以按策略分类进行离线分析,生成报表等等功能,这样交易执行程序和策略研究,报表生成等程序之间可以解耦合
        #================
        self._pm.on_fill_update(fill)
        await self._original_on_fill_update_callback(fill)

    @property
    def pm(self):
        """返回策略数据管理器
        """
        return self._pm

    def create_gateway(self, **kwargs):
        """ 创建一个交易网关.

        Args:
            strategy: 策略名称,由哪个策略发起
            platform: 交易平台
            symbols: 策略需要订阅和交易的币种
            account: 交易所登陆账号,如果为空就只是订阅市场公共行情数据,不进行登录认证,所以也无法进行交易等
            access_key: 登录令牌
            secret_key: 令牌密钥

            enable_kline_update: 是否启用`K线数据回调函数`(市场公共数据)
            enable_orderbook_update: 是否启用`深度数据通知回调函数`(市场公共数据)
            enable_trade_update: 是否启用`市场最新成交数据通知回调函数`(市场公共数据)
            enable_ticker_update: 是否启用`tick行情通知回调函数`(市场公共数据)
            enable_order_update: 是否启用`用户挂单通知回调函数`(用户私有数据)
            enable_fill_update: 是否启用`用户挂单成交通知回调函数`(用户私有数据)
            enable_position_update: 是否启用`用户仓位通知回调函数`(用户私有数据)
            enable_asset_update: 是否启用`用户资产通知回调函数`(用户私有数据)

            direct_kline_update: 直连交易所获取行情数据or订阅自己搭建的消息队列服务器获取行情数据
            direct_orderbook_update: 直连交易所获取行情数据or订阅自己搭建的消息队列服务器获取行情数据
            direct_trade_update: 直连交易所获取行情数据or订阅自己搭建的消息队列服务器获取行情数据
            direct_ticker_update: 直连交易所获取行情数据or订阅自己搭建的消息队列服务器获取行情数据

        Returns:
            交易网关实例
        """
        
        class CB(ExchangeGateway.ICallBack):
            async def on_kline_update_callback(self, kline: Kline): pass
            async def on_orderbook_update_callback(self, orderbook: Orderbook): pass
            async def on_trade_update_callback(self, trade: Trade): pass
            async def on_ticker_update_callback(self, ticker: Ticker): pass
            async def on_asset_update_callback(self, asset: Asset): pass
            async def on_position_update_callback(self, position: Position): pass
            async def on_order_update_callback(self, order: Order): pass
            async def on_fill_update_callback(self, fill: Fill): pass
            async def on_init_success_callback(self, success: bool, error: Error, **kwargs): pass

        cb = CB()
        cb.on_kline_update_callback = None
        cb.on_orderbook_update_callback = None
        cb.on_trade_update_callback = None
        cb.on_ticker_update_callback = None
        cb.on_asset_update_callback = None
        cb.on_position_update_callback = None
        cb.on_order_update_callback = None
        cb.on_fill_update_callback = None
        cb.on_init_success_callback = None

        #设置`初始化是否成功`回调通知函数
        cb.on_init_success_callback = self.on_init_success_callback
        
        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_kline_update"]:
            #直连交易所获取数据还是订阅自己搭建的消息队列服务器获取数据
            if kwargs["direct_kline_update"]:
                cb.on_kline_update_callback = self.on_kline_update_callback
            else:
                #从自己搭建的消息队列服务器订阅相对应的行情数据
                for sym in kwargs["symbols"]:
                    Market(const.MARKET_TYPE_KLINE, kwargs["platform"], sym, self.on_kline_update_callback)

        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_orderbook_update"]:
            #直连交易所获取数据还是订阅自己搭建的消息队列服务器获取数据
            if kwargs["direct_orderbook_update"]:
                cb.on_orderbook_update_callback = self.on_orderbook_update_callback
            else:
                #从自己搭建的消息队列服务器订阅相对应的行情数据
                for sym in kwargs["symbols"]:
                    Market(const.MARKET_TYPE_ORDERBOOK, kwargs["platform"], sym, self.on_orderbook_update_callback)

        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_trade_update"]:
            #直连交易所获取数据还是订阅自己搭建的消息队列服务器获取数据
            if kwargs["direct_trade_update"]:
                cb.on_trade_update_callback = self.on_trade_update_callback
            else:
                #从自己搭建的消息队列服务器订阅相对应的行情数据
                for sym in kwargs["symbols"]:
                    Market(const.MARKET_TYPE_TRADE, kwargs["platform"], sym, self.on_trade_update_callback)

        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_ticker_update"]:
            #直连交易所获取数据还是订阅自己搭建的消息队列服务器获取数据
            if kwargs["direct_ticker_update"]:
                cb.on_ticker_update_callback = self.on_ticker_update_callback
            else:
                #从自己搭建的消息队列服务器订阅相对应的行情数据
                for sym in kwargs["symbols"]:
                    Market(const.MARKET_TYPE_TICKER, kwargs["platform"], sym, self.on_ticker_update_callback)

        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_order_update"]:
            cb.on_order_update_callback = self.on_order_update_callback

        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_fill_update"]:
            cb.on_fill_update_callback = self.on_fill_update_callback

        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_position_update"]:
            cb.on_position_update_callback = self.on_position_update_callback

        #如果启用,就设置相对应的通知回调函数
        if kwargs["enable_asset_update"]:
            cb.on_asset_update_callback = self.on_asset_update_callback

        #下列参数已经没用,清除它们
        kwargs.pop("enable_kline_update")
        kwargs.pop("enable_orderbook_update")
        kwargs.pop("enable_trade_update")
        kwargs.pop("enable_ticker_update")
        kwargs.pop("enable_order_update")
        kwargs.pop("enable_fill_update")
        kwargs.pop("enable_position_update")
        kwargs.pop("enable_asset_update")
        kwargs.pop("direct_kline_update")
        kwargs.pop("direct_orderbook_update")
        kwargs.pop("direct_trade_update")
        kwargs.pop("direct_ticker_update")

        kwargs["cb"] = cb
        
        return Trader(**kwargs)

    def enable_timer(self, interval=1):
        """使能定时器功能
        """
        self._interval = interval
        SingleTask.call_later(self._later_call, self._interval)

    async def _later_call(self):
        """延时调用
        """
        await self.on_time()
        #继续开启下一个延迟过程,这种方式实现的定时器是串行模式,而不是并发模式,因为我们这里需要串行效果,这样适合策略编程
        SingleTask.call_later(self._later_call, self._interval)

    async def on_time(self):
        """定时器回调函数,子类需要继承实现这个函数
        """
        raise NotImplementedError
    
    async def create_pair_order(self, order1:TOrder, order2:TOrder):
        """ 同时创建一对订单,用于比如配对交易,统计套利,期现套利,搬砖套利等需要同时提交两个订单的情况

        Args:
            order1: 第一个订单参数
            order2: 第二个订单参数
            TOrder类型:
            [
            gateway: 交易网关
            symbol: Trade target
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.
            ]

        Returns:
            order_no[]: 订单ID列表,订单成功相应元素为订单ID,失败的话相应元素值为None.
            error[]: 错误信息列表,对应订单发生错误相应元素为错误信息,否则相应元素值为None.
        """
        obj1 = order1.gateway.create_order(order1.symbol, order1.action, order1.price, order1.quantity, order1.order_type)  #生成`协程对象`,并不是真正执行`协程函数`
        obj2 = order2.gateway.create_order(order2.symbol, order2.action, order2.price, order2.quantity, order2.order_type)  #生成`协程对象`,并不是真正执行`协程函数`
        l = [asyncio.create_task(obj1), asyncio.create_task(obj2)]
        done, pending = await asyncio.wait(l)
        success, error = [], []
        for t in done:
            success.append(t.result()[0])
            error.append(t.result()[1])
        return tuple(success), tuple(error)
    
    async def create_order(self, gateway, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, **kwargs):
        """ Create an order.

        Args:
            gateway: 交易网关
            symbol: Trade target
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await gateway.create_order(symbol, action, price, quantity, order_type, **kwargs)

    async def revoke_order(self, gateway, symbol, *order_nos):
        """ Revoke (an) order(s).

        Args:
            gateway: 交易网关
            symbol: Trade target
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel all orders for 
            this symbol. If you set 1 or multiple param, you can cancel an or multiple order.

        Returns:
            删除全部订单情况: 成功=(True, None), 失败=(False, error information)
            删除单个或多个订单情况: (删除成功的订单id[], 删除失败的订单id及错误信息[]),比如删除三个都成功那么结果为([1xx,2xx,3xx], [])
        """
        return await gateway.revoke_order(symbol, *order_nos)
    
    async def get_orders(self, gateway, symbol):
        """ 获取当前挂单列表

        Args:
            gateway: 交易网关
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await gateway.get_orders(symbol)
    
    async def get_assets(self, gateway):
        """ 获取交易账户资产信息

        Args:
            gateway: 交易网关

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await gateway.get_assets()
    
    async def get_position(self, gateway, symbol):
        """ 获取当前持仓

        Args:
            gateway: 交易网关
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await gateway.get_position(symbol)

    async def get_symbol_info(self, gateway, symbol):
        """ 获取指定符号相关信息

        Args:
            gateway: 交易网关
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await gateway.get_symbol_info(symbol)