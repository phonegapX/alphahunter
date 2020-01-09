# -*- coding:utf-8 -*-

"""
datamatrix_api

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


from quant.interface.infra_api import InfraAPI


class DataMatrixAPI:
    """ DataMatrixAPI
    """

    def __init__(self):
        """ 初始化
        """

    @staticmethod
    def today():
        """ 获取今天datetime
        """
        InfraAPI.today()

    @staticmethod
    def timenow():
        """ 获取现在时间datetime
        """
        InfraAPI.timenow()

    @staticmethod
    def timenow_str():
        """ 获取现在时间string
        """
        InfraAPI.timenow_str()

    @staticmethod
    def timenow_unix_time():
        """ 获取现在时间距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        InfraAPI.timenow_unix_time()

    @staticmethod
    def convert_unix_time(dt):
        """ datetime转换到距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        InfraAPI.convert_unix_time(dt)

    @staticmethod
    def convert_datetime(unix_time):
        """ 距离 Unix新纪元（1970年1月1日）的毫秒数转换为datetime
        """
        InfraAPI.convert_datetime(unix_time)

    @staticmethod
    def datetime_to_str(dt):
        """ datetime转换为string
        """
        InfraAPI.datetime_to_str(dt)

    @staticmethod
    def open_epoch_millisecond(date):
        """ 给定日期，找到那天的开盘毫秒数
        """
        InfraAPI.open_epoch_millisecond(date)

    @staticmethod
    def close_epoch_millisecond(date):
        """ 给定日期，找到那天的收盘毫秒数
        """
        InfraAPI.close_epoch_millisecond(date)
    
    @staticmethod
    async def get_research_usable_symbol_list():
        """ 从数据库获取可以用作研究的symbol列表
        """
        await InfraAPI.get_research_usable_symbol_list()
    
    @staticmethod
    async def get_trade_usable_symbol_list():
        """ 从数据库获取可以用作交易的symbol列表
        """
        await InfraAPI.get_trade_usable_symbol_list()

    @staticmethod
    async def get_kline_by_time(exchange, symbol, epoch_millisecond, kline_horizon, tolerance_millisecond):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定毫秒时间，容忍毫秒数，找到kline
        """
        await InfraAPI.get_kline_by_time(exchange, symbol, epoch_millisecond, kline_horizon, tolerance_millisecond)

    @staticmethod
    async def get_klines_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond, kline_horizon):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定起始毫秒，结束毫秒，找到所有kline列表
        """
        await InfraAPI.get_klines_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond, kline_horizon)

    @staticmethod
    async def get_prev_klines(exchange, symbol, epoch_millisecond, n, kline_horizon):
        """ 根据当前毫秒数，给定kline horizon，往过去load若干根kline
        """
        await InfraAPI.get_prev_klines(exchange, symbol, epoch_millisecond, n, kline_horizon)

    @staticmethod
    async def get_next_klines(exchange, symbol, epoch_millisecond, n, kline_horizon):
        """ 根据当前毫秒数，给定kline horizon，往未来load若干根kline
        """
        await InfraAPI.get_prev_klines(exchange, symbol, epoch_millisecond, n, kline_horizon)

    @staticmethod
    async def get_last_kline_oneday(exchange, symbol, date, kline_horizon):
        """ 给定日期，给定kline horizon，找到当天的最后一根kline
        """
        await InfraAPI.get_last_kline_oneday(exchange, symbol, date, kline_horizon)

    @staticmethod
    async def get_trade_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到trade
        """
        await InfraAPI.get_trade_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond)

    @staticmethod
    async def get_trades_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有trade列表
        """
        await InfraAPI.get_trades_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond)

    @staticmethod
    async def get_prev_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个trade
        """
        await InfraAPI.get_prev_trades(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_next_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个trade
        """
        await InfraAPI.get_prev_trades(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_last_trade_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一笔trade
        """
        await InfraAPI.get_last_trade_oneday(exchange, symbol, date)

    @staticmethod
    async def get_orderbook_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到orderbook
        """
        await InfraAPI.get_orderbook_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond)

    @staticmethod
    async def get_orderbooks_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有orderbook列表
        """
        await InfraAPI.get_orderbooks_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond)

    @staticmethod
    async def get_prev_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个orderbook
        """
        await InfraAPI.get_prev_orderbooks(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_next_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个orderbook
        """
        await InfraAPI.get_prev_orderbooks(exchange, symbol, epoch_millisecond, n)

    @staticmethod
    async def get_last_orderbook_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一个orderbook
        """
        await InfraAPI.get_last_orderbook_oneday(exchange, symbol, date)

    @staticmethod
    async def get_lead_ret_between_klines(exchange, symbol, kline1, kline2):
        """ 给定symbol，给定2个Kline，找到他们之间的lead_ret
        """
        await InfraAPI.get_lead_ret_between_klines(exchange, symbol, kline1, kline2)

    @staticmethod
    async def get_lag_ret_between_klines(exchange, symbol, kline1, kline2):
        """ 给定symbol，给定2个Kline，找到他们之间的lag_ret
        """
        await InfraAPI.get_lag_ret_between_klines(exchange, symbol, kline1, kline2)

    @staticmethod
    async def get_lead_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond):
        """ 给定symbol，给定2个毫秒时间，容忍毫秒数，找到他们之间的lead_ret
        """
        await InfraAPI.get_lead_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond)

    @staticmethod
    async def get_lag_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond):
        """ 给定symbol，给定2个毫秒时间，容忍毫秒数，找到他们之间的lag_ret
        """
        await InfraAPI.get_lag_ret_between_times(exchange, symbol, begin_millisecond, end_millisecond, tolerance_millisecond)
