# -*- coding:utf-8 -*-

"""
搬砖套利

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import asyncio

from quant import const
from quant.state import State
from quant.utils import tools, logger
from quant.config import config
from quant.market import Market, Kline, Orderbook, Trade, Ticker
from quant.order import Order, Fill, ORDER_ACTION_BUY, ORDER_ACTION_SELL, ORDER_STATUS_FILLED, ORDER_STATUS_PARTIAL_FILLED, ORDER_TYPE_MARKET, ORDER_TYPE_IOC
from quant.position import Position
from quant.asset import Asset
from quant.tasks import LoopRunTask
from quant.gateway import ExchangeGateway
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.interface.model_api import ModelAPI
from quant.startup import default_main


class CarryBrickStrategy(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(CarryBrickStrategy, self).__init__()
        
        self.strategy = config.strategy
        
        #=====================================================
        #创建[主交易所]交易接口
        self.platform_main = config.platforms[0]["platform"]
        self.account_main = config.platforms[0]["account"]
        access_key = config.platforms[0]["access_key"]
        secret_key = config.platforms[0]["secret_key"]
        self.symbols_main = config.platforms[0]["symbols"]

        #交易模块参数
        params = {
            "strategy": self.strategy,
            "platform": self.platform_main,
            "symbols": self.symbols_main,
            "account": self.account_main,
            "access_key": access_key,
            "secret_key": secret_key,

            "enable_kline_update": False,
            "enable_orderbook_update": True, #只接收深度行情
            "enable_trade_update": False,
            "enable_ticker_update": False,
            "enable_order_update": True,
            "enable_fill_update": True,
            "enable_position_update": False,
            "enable_asset_update": True,

            "direct_kline_update": False,
            "direct_orderbook_update": False, #故意不直连交易所获取行情,而是从rabbitMQ订阅(需要部署行情采集程序,目的就是一起整合测试)
            "direct_trade_update": False,
            "direct_ticker_update": False
        }
        self.trader_main = self.create_gateway(**params)
        
        #=====================================================
        #创建[从交易所]交易接口
        self.platform_reference = config.platforms[1]["platform"]
        self.account_reference = config.platforms[1]["account"]
        access_key = config.platforms[1]["access_key"]
        secret_key = config.platforms[1]["secret_key"]
        passphrase = config.platforms[1]["passphrase"]
        self.symbols_reference = config.platforms[1]["symbols"]
        
        #交易模块参数
        params = {
            "strategy": self.strategy,
            "platform": self.platform_reference,
            "symbols": self.symbols_reference,
            "account": self.account_reference,
            "access_key": access_key,
            "secret_key": secret_key,
            "passphrase": passphrase,

            "enable_kline_update": False,
            "enable_orderbook_update": True, #只接收深度行情
            "enable_trade_update": False,
            "enable_ticker_update": False,
            "enable_order_update": True,
            "enable_fill_update": True,
            "enable_position_update": False,
            "enable_asset_update": True,

            "direct_kline_update": False,
            "direct_orderbook_update": False, #故意不直连交易所获取行情,而是从rabbitMQ订阅(需要部署行情采集程序,目的就是一起整合测试)
            "direct_trade_update": False,
            "direct_ticker_update": False
        }
        self.trader_reference = self.create_gateway(**params)
        
        #==================================================================
        #注册定时器
        self.enable_timer()  #每隔1秒执行一次回调
        
        #==================================================================
        #huobi为[主交易所],okex为[从交易所]

        #配置两个交易所的手续费水平
        self.maker_fee_main = 0.001
        self.taker_fee_main = 0.002
        self.maker_fee_reference = 0.001
        self.taker_fee_reference = 0.002

        #配置两个交易所的最小价格变动单位
        self.price_tick_main = 0.01
        self.price_tick_reference = 0.01

        #配置两个交易所最小下单数量
        self.min_volume_main = 0.001
        self.min_volume_reference = 0.001

        #设置套利空间利润参数，每笔交易的理论利润如果大于0.0005就叫做有套利空间
        self.profit_level = 0.0005

        #设置每笔交易最大的下单量，例如，我们设置每笔最大下单eth0.2个，最大下单金额50USDT，下单金额占可用资产的最大下单比例0.2
        self.max_volume = 0.2
        self.max_amount = 50.0
        self.max_frac = 0.2

        #设置每笔下单量占盘口比例
        self.volume_frac1 = 0.05
        self.volume_frac2 = 0.1

        #账户初始资金数量以及可用资金数量
        self.eth_initial_main = None
        self.usdt_initial_main = None
        self.eth_initial_reference = None
        self.usdt_initial_reference = None
        self.eth_available_main = None
        self.usdt_available_main = None
        self.eth_available_reference = None
        self.usdt_available_reference = None

        #账户资产不平衡最大比例，即一旦账户资产不平衡比例超过这个值，我们不允许下单，即便有套利机会
        self.max_asset_imbalance_frac = 0.9

        #reference标的上一次更新时间及盘口数据
        self.last_update_time_reference = 0
        self.last_orderbook_reference = None

        #main标的订单簿数据最新更新时间及盘口数据
        self.last_update_time_main = 0
        self.last_orderbook_main = None

        #设置两个标的行情时间差距阀值，超过这个时间，代表时间差距太久，不要计算是否有套利空间，比如1秒钟，即1000毫秒
        self.max_time_diff = 1000

        #策略状态，1代表没有套利机会，不下单，2代表发现套利机会，maker端下单，等待fak成交并另一边taker追单
        self.running_status = 1

        #策略main标的端挂单数量, 当前套利挂单方向, 以及当前套利挂单价格
        self.limit_order_direction = None
        self.limit_order_volume = None
        self.limit_order_sell_price = None
        self.limit_order_buy_price = None
        self.market_order_sell_price = None
        self.market_order_buy_price = None
        self.slippage = 3.0
        
        self.main_order_id = None
        
        self.ready_main = False #[主交易所]是否已经准备好
        self.ready_reference = False #[从交易所]是否已经准备好

    async def on_time(self):
        """ 每秒钟执行一次.
        """
        logger.info("on_time ...", caller=self)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)
        
        if state.code == State.STATE_CODE_READY: #交易接口准备好
            if state.platform == self.platform_main:
                self.ready_main = True #[主交易所]准备好了
            elif state.platform == self.platform_reference:
                self.ready_reference = True #[从交易所]准备好了
        elif state.code == State.STATE_CODE_CONNECT_SUCCESS:    #交易接口连接成功
            pass
        elif state.code == State.STATE_CODE_CONNECT_FAILED:     #交易接口连接失败
            pass #不需要过于关注此状态,因为底层接口会自动重新连接
        elif state.code == State.STATE_CODE_DISCONNECT:         #交易接口连接断开
            if state.platform == self.platform_main:
                self.ready_main = False #比如说网络出问题或者其他原因导致连接断开,就得暂停交易(无需担心,系统会自动重新连接)
            elif state.platform == self.platform_reference:
                self.ready_reference = False
        elif state.code == State.STATE_CODE_RECONNECTING:       #交易接口重新连接中
            pass #比如说可以记录重连次数,如果一段时间内一直在重连可能交易所出问题,可以酌情处理,如结束本策略进程等
        elif state.code == State.STATE_CODE_PARAM_MISS:         #交易接口初始化过程缺少参数
            ... #收到此状态通知,证明无法正常初始化,应该结束本策略进程
            exit()
            return
        elif state.code == State.STATE_CODE_GENERAL_ERROR:      #交易接口常规错误
            ... #策略进程运行过程中如果收到某些错误通知,可以根据实际情况判断,比如可以做一些策略善后工作,然后结束本策略进程
            #exit()
            return

    def calc_asset_imbalance(self):
        """计算资产变动比例
        """
        ratio1 = abs(self.eth_initial_main - self.eth_available_main) / self.eth_initial_main
        ratio2 = abs(self.usdt_initial_main - self.usdt_available_main) / self.usdt_initial_main
        ratio3 = abs(self.eth_initial_reference - self.eth_available_reference) / self.eth_initial_reference
        ratio4 = abs(self.usdt_initial_reference - self.usdt_available_reference) / self.usdt_initial_reference
        if max(ratio1, ratio2, ratio3, ratio4) > self.max_asset_imbalance_frac:
            return True
        return False

    def calc_time_diff_delay(self):
        """计算时间差距
        """
        time_diff = self.last_update_time_main - self.last_update_time_reference
        if time_diff > self.max_time_diff:
            return True
        return False

    def check_buy_arb_space(self, buy_price, volume_frac):
        """ 检查买方向是否有套利空间(主买从卖)
        """
        bidprices_reference = [bid[0] for bid in self.last_orderbook_reference.bids]
        bidsizes_reference = [bid[1] for bid in self.last_orderbook_reference.bids]
        
        logger.debug("check_buy_arb_space:", "{}, {}".format(buy_price*(1.0+self.maker_fee_main+self.profit_level), bidprices_reference[0]*(1.0-self.taker_fee_reference-self.profit_level)), caller=self)

        total_volume = 0.0
        for i in range(20):
            if buy_price * (1.0 + self.maker_fee_main + self.profit_level) <= bidprices_reference[i] * (1.0 - self.taker_fee_reference - self.profit_level):
                total_volume += bidsizes_reference[i]
                sell_price = bidprices_reference[i]
                sell_level = i
            else:
                break
        if total_volume == 0:
            buy_side_arb_space = [None, None, None, None, None]
        else:
            # 账户最多能买的数量比例
            buy_volume1 = self.max_frac * min(self.usdt_available_main / buy_price, self.eth_available_reference)
            # 单笔最大金额限制
            buy_volume2 = self.max_amount / buy_price
            # 套利空间范围内，最大下单比例限制
            buy_volume3 = total_volume * volume_frac
            # 计算最终可下单数量
            buy_volume = min(buy_volume1, buy_volume2, buy_volume3, self.max_volume)
            sell_volume = buy_volume
            # 套利空间: main标的买价，main标的买数量，reference标的卖价，reference标的卖数量，计算套利空间在reference价格的档位
            if buy_volume < max(self.min_volume_main, self.min_volume_reference):
                buy_side_arb_space = [None, None, None, None, None] #超过最小下单限制
            else:
                buy_side_arb_space = [buy_price, buy_volume, sell_price, sell_volume, sell_level]
        #返回结果
        return buy_side_arb_space

    def check_sell_arb_space(self, sell_price, volume_frac):
        """ 检查卖方向是否有套利空间(主卖从买)
        """
        askprices_reference = [ask[0] for ask in self.last_orderbook_reference.asks]
        asksizes_reference = [ask[1] for ask in self.last_orderbook_reference.asks]
        
        logger.debug("check_sell_arb_space:", "{}, {}".format(sell_price*(1.0-self.maker_fee_main-self.profit_level), askprices_reference[0]*(1.0+self.taker_fee_reference+self.profit_level)), caller=self)
        
        total_volume = 0.0
        for i in range(20):
            if sell_price * (1.0 - self.maker_fee_main - self.profit_level) >= askprices_reference[i] * (1.0 + self.taker_fee_reference + self.profit_level):
                total_volume += asksizes_reference[i]
                buy_price = askprices_reference[i]
                buy_level = i
            else:
                break
        if total_volume == 0:
            sell_side_arb_space = [None, None, None, None, None]
        else:
            # 账户最多能卖的数量比例
            sell_volume1 = self.max_frac * min(self.eth_available_main, self.usdt_available_reference / buy_price)
            # 单笔最大金额限制
            sell_volume2 = self.max_amount / buy_price
            # 套利空间范围内，最大下单比例限制
            sell_volume3 = total_volume * volume_frac
            # 计算最终可下单数量
            sell_volume = min(sell_volume1, sell_volume2, sell_volume3, self.max_volume)
            buy_volume = sell_volume
            # 套利空间: main标的卖价，main标的卖数量，reference标的买价，reference标的买数量，计算套利空间在reference价格的档位
            if sell_volume < max(self.min_volume_main, self.min_volume_reference):
                sell_side_arb_space = [None, None, None, None, None] #超过最小下单限制
            else:
                sell_side_arb_space = [sell_price, sell_volume, buy_price, buy_volume, buy_level]
        #返回结果
        return sell_side_arb_space

    def check_arb_space(self, volume_frac):
        """ 检查是否有套利空间
        """
        askprices_main = self.last_orderbook_main.asks[0][0]
        bidprices_main = self.last_orderbook_main.bids[0][0]

        #卖单方向套利空间
        if self.running_status == 1:
            sell_price = askprices_main - self.price_tick_main
            if sell_price <= bidprices_main: #防止taker成交,这样可以节省手续费(是否必须?)
                sell_side_arb_space = [None, None, None, None, None]
            else:
                sell_side_arb_space = self.check_sell_arb_space(sell_price, volume_frac)
        elif self.running_status == 2:
            sell_price = self.limit_order_sell_price
            sell_side_arb_space = self.check_sell_arb_space(sell_price, volume_frac)

        #买单方向套利空间
        if self.running_status == 1:
            buy_price = bidprices_main + self.price_tick_main
            if buy_price >= askprices_main: #防止taker成交,这样可以节省手续费(是否必须?)
                buy_side_arb_space = [None, None, None, None, None]
            else:
                buy_side_arb_space = self.check_buy_arb_space(buy_price, volume_frac)
        elif self.running_status == 2:
            buy_price = self.limit_order_buy_price
            buy_side_arb_space = self.check_buy_arb_space(buy_price, volume_frac)

        #检查卖方向和买方向哪个套利空间更优，然后返回，如果两个方向都有机会，我们选择可交易数量大的返回
        if sell_side_arb_space[1] is None and buy_side_arb_space[1] is None:
            return 'empty', sell_side_arb_space, buy_side_arb_space
        elif sell_side_arb_space[1] is not None and buy_side_arb_space[1] is None:
            return 'sell', sell_side_arb_space, buy_side_arb_space
        elif sell_side_arb_space[1] is None and buy_side_arb_space[1] is not None:
            return 'buy', sell_side_arb_space, buy_side_arb_space
        elif sell_side_arb_space[1] is not None and buy_side_arb_space[1] is not None:
            if sell_side_arb_space[1] > buy_side_arb_space[1]:
                return 'sell', sell_side_arb_space, buy_side_arb_space
            else:
                return 'buy', sell_side_arb_space, buy_side_arb_space

    @async_method_locker("CarryBrickStrategy.on_orderbook_update_callback.locker", False)
    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄更新
        """
        logger.info("orderbook:", orderbook, caller=self)
        if not (self.ready_main and self.ready_reference):
            return #必须两个交易所都准备好,才能正常工作

        if orderbook.platform == self.platform_reference: #保存[从交易所]的行情
            self.last_update_time_reference = ModelAPI.timenow_unix_time()
            self.last_orderbook_reference = orderbook
        elif orderbook.platform == self.platform_main: #[主交易所]来行情了
            self.last_update_time_main = ModelAPI.timenow_unix_time()
            self.last_orderbook_main = orderbook
            #检查策略运行状态
            if self.running_status == 1:
                if self.calc_asset_imbalance():
                    logger.warn("Asset imbalance reaches maximum, can not continue !!!", caller=self)
                    return
                if self.calc_time_diff_delay():
                    logger.info("Orderbook time delay is bigger than maximum, can not continue !!!", caller=self)
                    return
                direction, sell_side_arb_space, buy_side_arb_space = self.check_arb_space(self.volume_frac2) #检查是否有套利空间
                if direction != 'empty':

                    if direction == 'sell': #卖方向上有套利机会([主交易所]卖, [从交易所]买)
                        self.limit_order_direction = direction
                        self.limit_order_sell_price = sell_side_arb_space[0]
                        self.limit_order_volume = sell_side_arb_space[1]
                        self.market_order_buy_price = sell_side_arb_space[2]
                        #sell_side_arb_space = [sell_price, sell_volume, buy_price, buy_volume, buy_level]
                        #需要在main标的挂单，sell，价格是sell_price, 数量是sell_volume, 挂单类型是FAK，
                        #即在下单之后，一直监听成交情况，遇到第一笔fill，我们记录main标的成交数量volume_filled, 
                        #然后马上对main标的剩余单子，全部撤单，同时，需要在reference标的，市价单提交订单，
                        #buy，数量是volume_filled，价格是self.market_order_buy_price + self.slippage
                        success, error = await self.create_order(self.trader_main, self.symbols_main[0], ORDER_ACTION_SELL, self.limit_order_sell_price, self.limit_order_volume)
                        if error != None:
                            return
                        self.main_order_id = success
 
                    elif direction == 'buy': #买方向上有套利机会([主交易所]买, [从交易所]卖)
                        self.limit_order_direction = direction
                        self.limit_order_buy_price = buy_side_arb_space[0]
                        self.limit_order_volume = buy_side_arb_space[1]
                        self.market_order_sell_price = buy_side_arb_space[2]
                        #buy_side_arb_space = [buy_price, buy_volume, sell_price, sell_volume, sell_level]
                        #需要在main标的挂单，buy，价格是buy_price, 数量是buy_volume, 挂单类型是FAK，
                        #即在下单之后，一直监听成交情况，遇到第一笔fill，我们记录main标的成交数量volume_filled, 
                        #然后马上对main标的剩余单子，全部撤单，同时，需要在reference标的，市价单提交订单，
                        #sell，数量是volume_filled, 价格是self.market_order_sell_price - self.slippage
                        success, error = await self.create_order(self.trader_main, self.symbols_main[0], ORDER_ACTION_BUY, self.limit_order_buy_price, self.limit_order_volume)
                        if error != None:
                            return
                        self.main_order_id = success

                    #更新策略运行状态
                    self.running_status = 2
            
            #因为我前面下单采用的是ioc类型的订单,这种类型订单的特点就是提交订单后如果不能马上(部分)成交就会立刻撤销订单
            #所以不需要下面的代码去执行撤单逻辑, 所以暂时先注释掉
            elif self.running_status == 2:
                _, sell_side_arb_space, buy_side_arb_space = self.check_arb_space(self.volume_frac1)
                if self.limit_order_direction == 'sell':
                    cond1 = sell_side_arb_space[0] is None #套利空间已经消失
                    cond2 = sell_side_arb_space[0] is not None and sell_side_arb_space[1] < self.limit_order_volume #实际挂单数量已经大于可套利数量
                    cond3 = self.last_orderbook_main.asks[0][0] < self.limit_order_sell_price #挂单已经不是第一档位
                    if cond1 or cond2 or cond3:
                        #接下来需要对main标的撤单, 并对策略状态相关参数归位
                        await self.revoke_order(self.trader_main, self.symbols_main[0], self.main_order_id)
                        self.running_status = 1

                elif self.limit_order_direction == 'buy':
                    cond1 = buy_side_arb_space[0] is None #套利空间已经消失
                    cond2 = buy_side_arb_space[0] is not None and buy_side_arb_space[1] < self.limit_order_volume #实际挂单数量已经大于可套利数量
                    cond3 = self.last_orderbook_main.bids[0][0] > self.limit_order_buy_price #挂单已经不是第一档位
                    if cond1 or cond2 or cond3:
                        #接下来需要对main标的撤单, 并对策略状态相关参数归位
                        await self.revoke_order(self.trader_main, self.symbols_main[0], self.main_order_id)
                        self.running_status = 1

    async def on_kline_update_callback(self, kline: Kline): ...
    async def on_trade_update_callback(self, trade: Trade): ...
    async def on_ticker_update_callback(self, ticker: Ticker): ...

    async def on_order_update_callback(self, order: Order):
        """ 订单状态更新
        """
        logger.info("order:", order, caller=self)
        if order.platform == self.platform_main and self.main_order_id == order.order_no:
            if order.status in [ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED]:
                #把[主交易所]剩余订单部分撤单
                if order.status == ORDER_STATUS_PARTIAL_FILLED:
                    asyncio.create_task(self.revoke_order(self.trader_main, self.symbols_main[0], self.main_order_id)) #这里代码不会阻塞,而是和下面的代码并发执行
                #[从交易所]下单
                volume_filled = order.quantity - order.remain
                if self.limit_order_direction == "sell": #[主交易所]挂的是卖单，那么[从交易所]就要下买单
                    #尽力促成成交,可以理解为用IOC单外加滑点去模拟市价单,原因是'市价买单'类型不能直接传入'货数量'这个参数(IOC的好处是不需要考虑撤单逻辑)
                    #当然还是有不能成交的风险,如果没成交,要不重新下单,要不干脆不理,等专门的平衡模块去处理
                    buy_price = self.market_order_buy_price + self.slippage
                    success, error = await self.create_order(self.trader_reference, self.symbols_reference[0], ORDER_ACTION_BUY, buy_price, volume_filled, ORDER_TYPE_IOC)
                    if error != None:
                        #如果下单失败了,可以重新下单,或者不管了,等专门的平衡模块去处理
                        logger.error("下单失败:", error, caller=self)
                    #amount = self.market_order_buy_price * volume_filled
                    #success, error = await self.create_order(self.trader_reference, self.symbols_reference[0], ORDER_ACTION_BUY, 0, amount, ORDER_TYPE_MARKET)
                elif self.limit_order_direction == "buy": #[主交易所]挂的是买单，那么[从交易所]就要下卖单
                    #sell_price = self.market_order_sell_price - self.slippage
                    #可以直接用'市价卖单',因为'市价卖单'可以直接传入'货数量'这个参数
                    success, error = await self.create_order(self.trader_reference, self.symbols_reference[0], ORDER_ACTION_SELL, 0, volume_filled, ORDER_TYPE_MARKET)
                    if error != None:
                        #如果下单失败了,可以重新下单,或者不管了,等专门的平衡模块去处理
                        logger.error("下单失败:", error, caller=self)
            #判断是否需要将策略状态复位
            if order.status in [ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED]:
                self.running_status = 1

    async def on_fill_update_callback(self, fill: Fill):
        """ 订单成交通知
        """
        logger.info("fill:", fill, caller=self)

    async def on_asset_update_callback(self, asset: Asset):
        """ 账户资产更新
        """
        logger.info("asset:", asset, caller=self)
        
        balance = asset.assets #e.g. {"BTC": {"free": 1.1, "locked": 2.2, "total": 3.3}, ... }
        if asset.platform == self.platform_main: #获取[主交易所]的资产信息
            self.eth_available_main = balance["eth"]["free"] #huobi交易所用的是小写符号
            self.usdt_available_main = balance["usdt"]["free"]
            if not self.eth_initial_main:
                self.eth_initial_main = self.eth_available_main #策略开始时的初始资产
            if not self.usdt_initial_main:
                self.usdt_initial_main = self.usdt_available_main #策略开始时的初始资产
        elif asset.platform == self.platform_reference: #获取[从交易所]的资产信息
            self.eth_available_reference = balance["ETH"]["free"] #okex交易所用的是大写符号
            self.usdt_available_reference = balance["USDT"]["free"]
            if not self.eth_initial_reference:
                self.eth_initial_reference = self.eth_available_reference #策略开始时的初始资产
            if not self.usdt_initial_reference:
                self.usdt_initial_reference = self.usdt_available_reference #策略开始时的初始资产

    async def on_position_update_callback(self, position: Position): ...


if __name__ == '__main__':
    default_main(CarryBrickStrategy)
