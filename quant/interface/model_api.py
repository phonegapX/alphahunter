# -*- coding:utf-8 -*-

"""
model_api

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.config import config
from quant.feed import HistoryDataFeed
from quant.interface.infra_api import InfraAPI


class ModelAPI:
    """ ModelAPI
    """
    def __init__(self):
        """ 初始化
        """

    @staticmethod
    def today():
        """ 获取今天datetime
        """
        if config.backtest or config.datamatrix: #如果是回测模式或者数据矩阵模式
            #回测环境中的"当前时间"
            ts = HistoryDataFeed.current_milli_timestamp()
            return InfraAPI.milli_timestamp_to_datetime(ts).date()
        else:
            return InfraAPI.today()

    @staticmethod
    def current_datetime():
        """ 获取现在时间datetime
        """
        if config.backtest or config.datamatrix: #如果是回测模式或者数据矩阵模式
            #回测环境中的"当前时间"
            ts = HistoryDataFeed.current_milli_timestamp()
            return InfraAPI.milli_timestamp_to_datetime(ts)
        else:
            return InfraAPI.current_datetime()

    @staticmethod
    def current_milli_timestamp():
        """ 获取现在时间距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        if config.backtest or config.datamatrix: #如果是回测模式或者数据矩阵模式
            #回测环境中的"当前时间"
            return HistoryDataFeed.current_milli_timestamp()
        else:
            return InfraAPI.current_milli_timestamp()

    @staticmethod
    def datetime_to_milli_timestamp(dt):
        """ datetime转换到距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        return InfraAPI.datetime_to_milli_timestamp(dt)

    @staticmethod
    def milli_timestamp_to_datetime(ts):
        """ 距离 Unix新纪元（1970年1月1日）的毫秒数转换为datetime
        """
        return InfraAPI.milli_timestamp_to_datetime(ts)

    @staticmethod
    def datetime_to_str(dt, fmt='%Y-%m-%d %H:%M:%S.%f'):
        """ datetime转换为string
        """
        return InfraAPI.datetime_to_str(dt, fmt)

    @staticmethod
    def datetime_delta_time(dt, delta_day=0, delta_minute=0, delta_second=0):
        """
        """
        return InfraAPI.datetime_delta_time(dt, delta_day, delta_minute, delta_second)

    @staticmethod
    def find_last_datetime_by_time_str(ts, reference_time_str='12:00:00.000'):
        """
        """
        return InfraAPI.find_last_datetime_by_time_str(ts, reference_time_str)

    @staticmethod
    def open_epoch_millisecond(date):
        """ 给定日期，找到那天的开盘毫秒数
        """
        return InfraAPI.open_epoch_millisecond(date)

    @staticmethod
    def close_epoch_millisecond(date):
        """ 给定日期，找到那天的收盘毫秒数
        """
        return InfraAPI.close_epoch_millisecond(date)
    
    @staticmethod
    async def get_research_usable_symbol_list():
        """ 从数据库获取可以用作研究的symbol列表
        """
        return await InfraAPI.get_research_usable_symbol_list()
    
    @staticmethod
    async def get_trade_usable_symbol_list():
        """ 从数据库获取可以用作交易的symbol列表
        """
        return await InfraAPI.get_trade_usable_symbol_list()

    @staticmethod
    async def get_kline_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond, kline_horizon=None):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定毫秒时间，容忍毫秒数，找到kline
        """
        return await InfraAPI.get_kline_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond, kline_horizon)

    @staticmethod
    async def get_klines_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond, kline_horizon=None):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定起始毫秒，结束毫秒，找到所有kline列表
        """
        return await InfraAPI.get_klines_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond, kline_horizon)

    @staticmethod
    async def get_prev_klines(exchange, symbol, epoch_millisecond, n, kline_horizon=None):
        """ 根据当前毫秒数，给定kline horizon，往过去load若干根kline
        """
        return await InfraAPI.get_prev_klines(exchange, symbol, epoch_millisecond, n, kline_horizon)

    @staticmethod
    async def get_next_klines(exchange, symbol, epoch_millisecond, n, kline_horizon=None):
        """ 根据当前毫秒数，给定kline horizon，往未来load若干根kline
        """
        return await InfraAPI.get_next_klines(exchange, symbol, epoch_millisecond, n, kline_horizon)

    @staticmethod
    async def get_last_kline_oneday(exchange, symbol, date, kline_horizon=None):
        """ 给定日期，给定kline horizon，找到当天的最后一根kline
        """
        return await InfraAPI.get_last_kline_oneday(exchange, symbol, date, kline_horizon)

    @staticmethod
    async def get_trade_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到trade
        """
        return await InfraAPI.get_trade_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond)

    @staticmethod
    async def get_trades_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有trade列表
        """
        return await InfraAPI.get_trades_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond)

    @staticmethod
    async def get_prev_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个trade
        """
        return await InfraAPI.get_prev_trades(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_next_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个trade
        """
        return await InfraAPI.get_next_trades(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_last_trade_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一笔trade
        """
        return await InfraAPI.get_last_trade_oneday(exchange, symbol, date)

    @staticmethod
    async def get_orderbook_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到orderbook
        """
        return await InfraAPI.get_orderbook_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond)

    @staticmethod
    async def get_orderbooks_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有orderbook列表
        """
        return await InfraAPI.get_orderbooks_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond)

    @staticmethod
    async def get_prev_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个orderbook
        """
        return await InfraAPI.get_prev_orderbooks(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_next_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个orderbook
        """
        return await InfraAPI.get_next_orderbooks(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_last_orderbook_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一个orderbook
        """
        return await InfraAPI.get_last_orderbook_oneday(exchange, symbol, date)

    @staticmethod
    async def get_lead_ret_between_klines(exchange, symbol, kline1, kline2):
        """ 给定symbol，给定2个Kline，找到他们之间的lead_ret
        """
        return await InfraAPI.get_lead_ret_between_klines(exchange, symbol, kline1, kline2)

    @staticmethod
    async def get_lag_ret_between_klines(exchange, symbol, kline1, kline2):
        """ 给定symbol，给定2个Kline，找到他们之间的lag_ret
        """
        return await InfraAPI.get_lag_ret_between_klines(exchange, symbol, kline1, kline2)

    @staticmethod
    async def get_lead_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond):
        """ 给定symbol，给定2个毫秒时间，容忍毫秒数，找到他们之间的lead_ret
        """
        return await InfraAPI.get_lead_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond)

    @staticmethod
    async def get_lag_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond):
        """ 给定symbol，给定2个毫秒时间，容忍毫秒数，找到他们之间的lag_ret
        """
        return await InfraAPI.get_lag_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond)