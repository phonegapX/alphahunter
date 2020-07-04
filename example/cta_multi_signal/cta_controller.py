# -*- coding:utf-8 -*-

"""
固定数量模式CTA主策略演示

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import copy
import asyncio

from abc import ABCMeta, abstractmethod

from quant import const
from quant.state import State
from quant.utils import tools, logger
from quant.config import config
from quant.market import Market, Kline, Orderbook, Trade, Ticker
from quant.order import Order, Fill, ORDER_ACTION_BUY, ORDER_ACTION_SELL, ORDER_STATUS_FILLED, ORDER_TYPE_MARKET, ORDER_TYPE_IOC
from quant.position import Position
from quant.asset import Asset
from quant.tasks import LoopRunTask, SingleTask
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.interface.model_api import ModelAPI

from nr import RsiModel, CciModel, MaModel


class CTAController(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(CTAController, self).__init__()

        #指定这个主策略下面管理了NrModel这个模型
        self.models = [RsiModel(), CciModel(), MaModel()]

        #NrModel关心‘btc’，所以主策略也会关心
        self.coins = ['BTC']

        #主策略计价货币：指定这个主策略是以usdt为计价货币
        self.quote_asset = 'USDT'

        #初始资产列表
        self.init_assets = None

        #当前仓位
        self.current_position = {
            'BTC': 0
        }

        #目标仓位
        self.target_position = {
            'BTC': 0
        }

        #主策略差值仓位，即需要调仓的部分：当前仓位距离目标仓位的差距
        self.delta_position = {
            'BTC': 0
        }

        #是否需要停止策略
        self.is_stop = False

    def model_consolidation(self):
        """ 模型组合
        """
        is_stop = True
        #根据各个模块,统计总的目标仓位
        for model in self.models:
            for coin in self.coins:
                self.target_position[coin] = 0
            for coin in self.coins:
                self.target_position[coin] += model.target_position[coin]
            #只有所有model都是停止状态后策略才会停止,只要有一个model还在运行,就不能停止
            if model.running_status == 'running':
                is_stop = False
        self.is_stop = is_stop
        #计算当前仓位距离目标仓位的距离，即调仓目标
        for coin in self.coins:
            self.delta_position[coin] = self.target_position[coin] - self.current_position[coin]

    async def on_time(self):
        """ 每5秒执行一次
        """
        #对model循环，获取model最新信号
        for model in self.models:
            await model.on_time()

        #调用model_consolidation, 更新目标仓位等信息
        self.model_consolidation()

        #根据差值仓位来下单
        await self.submit_orders(self.delta_position)

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        #对model循环，获取model最新信号
        for model in self.models:
            await model.on_kline_update_callback(kline)

        #调用model_consolidation, 更新目标仓位等信息
        self.model_consolidation()

        #根据差值仓位来下单
        await self.submit_orders(self.delta_position)

    async def on_asset_update_callback(self, asset: Asset):
        """ 账户资产更新
        """
        if not self.init_assets:
            self.init_assets = copy.deepcopy(asset.assets) #初始资产列表,只会获取一次
        #获取当前持仓
        for coin in self.coins:
            self.current_position[coin] = asset.assets[coin]["total"] - self.init_assets[coin]["total"]

    @abstractmethod
    async def submit_orders(self, delta_position):
        """ 根据当前最新的self.delta_position来执行下单操作
        """