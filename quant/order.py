# -*- coding:utf-8 -*-

"""
Order object and Fill object.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.utils import tools

# maker or taker
LIQUIDITY_TYPE_TAKER = "TAKER"
LIQUIDITY_TYPE_MAKER = "MAKER"

# Order type.
ORDER_TYPE_LIMIT = "LIMIT"  # Limit order.
ORDER_TYPE_MARKET = "MARKET"  # Market order.

# Order direction.
ORDER_ACTION_BUY = "BUY"  # Buy
ORDER_ACTION_SELL = "SELL"  # Sell

# Order status.
ORDER_STATUS_NONE = "NONE"  # New created order, no status.
ORDER_STATUS_SUBMITTED = "SUBMITTED"  # The order that submitted to server successfully.
ORDER_STATUS_PARTIAL_FILLED = "PARTIAL-FILLED"  # The order that filled partially.
ORDER_STATUS_FILLED = "FILLED"  # The order that filled fully.
ORDER_STATUS_CANCELED = "CANCELED"  # The order that canceled.
ORDER_STATUS_FAILED = "FAILED"  # The order that failed.

# Future order trade type.
TRADE_TYPE_NONE = 0  # Unknown type, some Exchange's order information couldn't known the type of trade.
TRADE_TYPE_BUY_OPEN = 1  # Buy open, action = BUY & quantity > 0.
TRADE_TYPE_SELL_OPEN = 2  # Sell open, action = SELL & quantity < 0.
TRADE_TYPE_SELL_CLOSE = 3  # Sell close, action = SELL & quantity > 0.
TRADE_TYPE_BUY_CLOSE = 4  # Buy close, action = BUY & quantity < 0.


class Order:
    """ Order object.

    Attributes:
        account: Trading account name, e.g. test@gmail.com.
        platform: Exchange platform name, e.g. binance/bitmex.
        strategy: Strategy name, e.g. my_test_strategy.
        order_no: order id.
        symbol: Trading pair name, e.g. ETH/BTC.
        action: Trading side, BUY/SELL.
        price: Order price.
        quantity: Order quantity.
        remain: Remain quantity that not filled.
        status: Order status.
        avg_price: Average price that filled.
        order_type: Order type.
        trade_type: Trade type, only for future order.
        ctime: Order create time, millisecond.
        utime: Order update time, millisecond.
    """

    def __init__(self, account=None, platform=None, strategy=None, order_no=None, symbol=None, action=None, price=0,
                 quantity=0, remain=0, status=ORDER_STATUS_NONE, avg_price=0, order_type=ORDER_TYPE_LIMIT,
                 trade_type=TRADE_TYPE_NONE, ctime=None, utime=None):
        self.platform = platform
        self.account = account
        self.strategy = strategy
        self.order_no = order_no
        self.action = action
        self.order_type = order_type
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.remain = remain
        self.status = status
        self.avg_price = avg_price
        self.trade_type = trade_type
        self.ctime = ctime if ctime else tools.get_cur_timestamp_ms()
        self.utime = utime if utime else tools.get_cur_timestamp_ms()

    def __str__(self):
        info = "[platform: {platform}, account: {account}, strategy: {strategy}, order_no: {order_no}, " \
               "action: {action}, symbol: {symbol}, price: {price}, quantity: {quantity}, remain: {remain}, " \
               "status: {status}, avg_price: {avg_price}, order_type: {order_type}, trade_type: {trade_type}, " \
               "ctime: {ctime}, utime: {utime}]".format(
            platform=self.platform, account=self.account, strategy=self.strategy, order_no=self.order_no,
            action=self.action, symbol=self.symbol, price=self.price, quantity=self.quantity,
            remain=self.remain, status=self.status, avg_price=self.avg_price, order_type=self.order_type,
            trade_type=self.trade_type, ctime=self.ctime, utime=self.utime)
        return info

    def __repr__(self):
        return str(self)


class Fill:
    """ Fill object.

    Attributes:
        platform: Exchange platform name, e.g. binance/bitmex.
        account: Trading account name, e.g. test@gmail.com.
        symbol: Trading pair name, e.g. ETH/BTC.
        strategy: Strategy name, e.g. my_test_strategy.
        order_no: 哪个订单发生了成交
        fill_no: 成交ID
        price: 成交价格
        quantity: 成交数量
        side: 成交方向买还是卖
        liquidity: 是maker成交还是taker成交
        fee: 成交手续费
        ctime: 成交时间
    """

    def __init__(self, platform=None, account=None, symbol=None, strategy=None, order_no=None, fill_no=None, 
                 price=0, quantity=0, side=None, liquidity=None, fee=0, ctime=None):
        self.platform = platform
        self.account = account
        self.symbol = symbol
        self.strategy = strategy
        self.order_no = order_no
        self.fill_no = fill_no
        self.price = price
        self.quantity = quantity
        self.side = side
        self.liquidity = liquidity
        self.fee = fee
        self.ctime = ctime if ctime else tools.get_cur_timestamp_ms()

    def __str__(self):
        info = "[platform: {platform}, account: {account}, symbol: {symbol}, strategy: {strategy}, order_no: {order_no}, " \
               "fill_no: {fill_no}, price: {price}, quantity: {quantity}, side: {side}, " \
               "liquidity: {liquidity}, fee: {fee}, ctime: {ctime}]".format(
            platform=self.platform, account=self.account, symbol=self.symbol, strategy=self.strategy, order_no=self.order_no,
            fill_no=self.fill_no,  price=self.price, quantity=self.quantity, side=self.side, 
            liquidity=self.liquidity, fee=self.fee, ctime=self.ctime)
        return info

    def __repr__(self):
        return str(self)


class SymbolInfo:
    """ 符号的相关信息

    Attributes:
        platform: Exchange platform name, e.g. binance/bitmex.
        symbol: Trading pair name, e.g. ETH/BTC.
        price_tick: `报价`每一跳的最小单位,也可以理解为`报价`精度或者`报价`增量
        size_tick: `下单量`每一跳的最小单位,也可以理解为`下单量`精度或者`下单量`增量
        size_limit: 最小`下单量` (`下单量`指当订单类型为`限价单`或`sell-market`时,下单接口传的'quantity')
        value_tick: `下单金额`每一跳的最小单位,也可以理解为`下单金额`精度或者`下单金额`增量
        value_limit: 最小`下单金额` (`下单金额`指当订单类型为`限价单`时,下单接口传入的(quantity * price).当订单类型为`buy-market`时,下单接口传的'quantity')
    """
    
    def __init__(self, platform=None, symbol=None, price_tick:float=None, size_tick:float=None, size_limit:float=None, value_tick:float=None, value_limit:float=None):
        self.platform = platform
        self.symbol = symbol
        self.price_tick = price_tick
        self.size_tick = size_tick
        self.size_limit = size_limit
        self.value_tick = value_tick
        self.value_limit = value_limit
        
    def __str__(self):
        info = "[platform: {platform}, symbol: {symbol}, price_tick: {price_tick}, size_tick: {size_tick}, size_limit: {size_limit}, value_tick: {value_tick}, value_limit: {value_limit}]".format(
            platform=self.platform, symbol=self.symbol, price_tick=self.price_tick, size_tick=self.size_tick, size_limit=self.size_limit, value_tick=self.value_tick, value_limit=self.value_limit)
        return info

    def __repr__(self):
        return str(self)