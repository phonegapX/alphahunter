# -*- coding:utf-8 -*-

"""
infra_api

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


class InfraAPI:
    """ 基础历史行情API
    """

    def __init__(self):
        """ 初始化
        """

    @staticmethod
    def today():
        """ 获取今天datetime
        """
        pass

    @staticmethod
    def timenow():
        """ 获取现在时间datetime
        """
        pass

    @staticmethod
    def timenow_str():
        """ 获取现在时间string
        """
        pass

    @staticmethod
    def timenow_unix_time():
        """ 获取现在时间距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        pass

    @staticmethod
    def convert_unix_time(dt):
        """ datetime转换到距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        pass

    @staticmethod
    def convert_datetime(unix_time):
        """ 距离 Unix新纪元（1970年1月1日）的毫秒数转换为datetime
        """
        pass

    @staticmethod
    def datetime_to_str(dt):
        """ datetime转换为string
        """
        pass

    @staticmethod
    def open_epoch_millisecond(date):
        """ 给定日期，找到那天的开盘毫秒数
        """
        pass

    @staticmethod
    def close_epoch_millisecond(date):
        """ 给定日期，找到那天的收盘毫秒数
        """
        pass
    
    @staticmethod
    async def get_research_usable_symbol_list():
        """ 从数据库获取可以用作研究的symbol列表
        """
        pass
    
    @staticmethod
    async def get_trade_usable_symbol_list():
        """ 从数据库获取可以用作交易的symbol列表
        """
        pass

    @staticmethod
    async def get_kline_by_time(exchange, symbol, epoch_millisecond, kline_horizon, tolerance_millisecond):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定毫秒时间，容忍毫秒数，找到kline
        """
        pass

    @staticmethod
    async def get_klines_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond, kline_horizon):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定起始毫秒，结束毫秒，找到所有kline列表
        """
        pass

    @staticmethod
    async def get_prev_klines(exchange, symbol, epoch_millisecond, n, kline_horizon):
        """ 根据当前毫秒数，给定kline horizon，往过去load若干根kline
        """
        pass

    @staticmethod
    async def get_next_klines(exchange, symbol, epoch_millisecond, n, kline_horizon):
        """ 根据当前毫秒数，给定kline horizon，往未来load若干根kline
        """
        pass

    @staticmethod
    async def get_last_kline_oneday(exchange, symbol, date, kline_horizon):
        """ 给定日期，给定kline horizon，找到当天的最后一根kline
        """
        pass

    @staticmethod
    async def get_trade_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到trade
        """
        pass

    @staticmethod
    async def get_trades_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有trade列表
        """
        pass

    @staticmethod
    async def get_prev_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个trade
        """
        pass

    @staticmethod
    async def get_next_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个trade
        """
        pass

    @staticmethod
    async def get_last_trade_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一笔trade
        """
        pass

    @staticmethod
    async def get_orderbook_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到orderbook
        """
        pass

    @staticmethod
    async def get_orderbooks_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有orderbook列表
        """
        pass

    @staticmethod
    async def get_prev_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个orderbook
        """
        pass

    @staticmethod
    async def get_next_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个orderbook
        """
        pass

    @staticmethod
    async def get_last_orderbook_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一个orderbook
        """
        pass

    @staticmethod
    async def get_lead_ret_between_klines(exchange, symbol, kline1, kline2):
        """ 给定symbol，给定2个Kline，找到他们之间的lead_ret
        """
        pass

    @staticmethod
    async def get_lag_ret_between_klines(exchange, symbol, kline1, kline2):
        """ 给定symbol，给定2个Kline，找到他们之间的lag_ret
        """
        pass

    @staticmethod
    async def get_lead_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond):
        """ 给定symbol，给定2个毫秒时间，容忍毫秒数，找到他们之间的lead_ret
        """
        pass

    @staticmethod
    async def get_lag_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond):
        """ 给定symbol，给定2个毫秒时间，容忍毫秒数，找到他们之间的lag_ret
        """
        pass
