# -*- coding:utf-8 -*-

"""
infra_api

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import time
import datetime
from collections import defaultdict

import pymongo

from quant.utils.mongo import MongoDB


class InfraAPI:
    """ 基础历史行情API
    """
    
    t_depth_map = defaultdict(lambda:None)
    t_trade_map = defaultdict(lambda:None)
    t_kline_map = defaultdict(lambda:None)
    
    def __init__(self):
        """ 初始化
        """

    @staticmethod
    def _get_db_depth_reader(exchange, symbol):
        postfix = symbol.replace('-','').replace('_','').replace('/','').lower() #将所有可能的情况转换为我们自定义的数据库表名规则
        if not InfraAPI.t_depth_map[symbol]:
            #订单薄
            name = "t_orderbook_{}_{}".format(exchange, postfix).lower()
            InfraAPI.t_depth_map[symbol] = MongoDB("db_market", name)
        return InfraAPI.t_depth_map[symbol]

    @staticmethod
    def _get_db_trade_reader(exchange, symbol):
        postfix = symbol.replace('-','').replace('_','').replace('/','').lower() #将所有可能的情况转换为我们自定义的数据库表名规则
        if not InfraAPI.t_trade_map[symbol]:
            #逐笔成交
            name = "t_trade_{}_{}".format(exchange, postfix).lower()
            InfraAPI.t_trade_map[symbol] = MongoDB("db_market", name)
        return InfraAPI.t_trade_map[symbol]

    @staticmethod
    def _get_db_kline_reader(exchange, symbol):
        if not InfraAPI.t_kline_map[symbol]:
            postfix = symbol.replace('-','').replace('_','').replace('/','').lower() #将所有可能的情况转换为我们自定义的数据库表名规则
            #K线
            name = "t_kline_{}_{}".format(exchange, postfix).lower()
            InfraAPI.t_kline_map[symbol] = MongoDB("db_custom_kline", name)
        return InfraAPI.t_kline_map[symbol]

    @staticmethod
    def today():
        """ 获取今天datetime
        """
        return datetime.date.today()

    @staticmethod
    def current_datetime():
        """ 获取现在时间datetime
        """
        return datetime.datetime.now()

    @staticmethod
    def current_milli_timestamp():
        """ 获取现在时间距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        return InfraAPI.datetime_to_milli_timestamp(InfraAPI.current_datetime())

    @staticmethod
    def datetime_to_milli_timestamp(dt):
        """ datetime转换到距离 Unix新纪元（1970年1月1日）的毫秒数
        """
        t = dt.timetuple()
        ts = int(time.mktime(t))*1000 + round(dt.microsecond/1000)
        return ts

    @staticmethod
    def milli_timestamp_to_datetime(ts):
        """ 距离 Unix新纪元（1970年1月1日）的毫秒数转换为datetime
        """
        dt = datetime.datetime.fromtimestamp(ts/1000)
        return dt

    @staticmethod
    def datetime_to_str(dt, fmt='%Y-%m-%d %H:%M:%S.%f'):
        """ datetime转换为string
        """
        return dt.strftime(fmt)

    @staticmethod
    def datetime_delta_time(dt, delta_day=0, delta_minute=0, delta_second=0):
        """
        """
        dt += datetime.timedelta(days=delta_day, minutes=delta_minute, seconds=delta_second)
        return dt

    @staticmethod
    def find_last_datetime_by_time_str(ts, reference_time_str='12:00:00.000'):
        """
        """
        dt = InfraAPI.milli_timestamp_to_datetime(ts)
        date_str = InfraAPI.datetime_to_str(dt)
        day_str = date_str[0:10]
        time_str = date_str[11:]
        if time_str >= reference_time_str:
            return day_str + ' ' + reference_time_str
        else:
            dt = InfraAPI.datetime_delta_time(dt, delta_day=-1)
            date_str = InfraAPI.datetime_to_str(dt)
            day_str = date_str[0:10]
            return day_str + ' ' + reference_time_str

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
    async def get_kline_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond=0, kline_horizon=None):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定毫秒时间，容忍毫秒数，找到kline
        """
        cursor = InfraAPI._get_db_kline_reader(exchange, symbol)
        s, e = await cursor.find_one({'begin_dt':{'$gte':epoch_millisecond,'$lt':epoch_millisecond+tolerance_millisecond+1}})
        if e:
            return None
        return s

    @staticmethod
    async def get_klines_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond, kline_horizon=None):
        """ 根据给定symbol，给定kline horizon，比如1min或者5min，给定起始毫秒，结束毫秒，找到所有kline列表
        """
        cursor = InfraAPI._get_db_kline_reader(exchange, symbol)
        s, e = await cursor.get_list({'begin_dt':{'$gte':begin_epoch_millisecond,'$lt':end_epoch_millisecond}})
        if e:
            return None
        return s

    @staticmethod
    async def get_prev_klines(exchange, symbol, epoch_millisecond, n, kline_horizon=None):
        """ 根据当前毫秒数，给定kline horizon，往过去load若干根kline
        """
        cursor = InfraAPI._get_db_kline_reader(exchange, symbol)
        sort = [('begin_dt', pymongo.DESCENDING)]
        s, e = await cursor.get_list({'begin_dt':{'$lt':epoch_millisecond}}, sort=sort, limit=n)
        if e:
            return None
        return s

    @staticmethod
    async def get_next_klines(exchange, symbol, epoch_millisecond, n, kline_horizon=None):
        """ 根据当前毫秒数，给定kline horizon，往未来load若干根kline
        """
        cursor = InfraAPI._get_db_kline_reader(exchange, symbol)
        s, e = await cursor.get_list({'begin_dt':{'$gte':epoch_millisecond}}, limit=n)
        if e:
            return None
        return s

    @staticmethod
    async def get_last_kline_oneday(exchange, symbol, date, kline_horizon=None):
        """ 给定日期，给定kline horizon，找到当天的最后一根kline
        """
        ONE_DAY = 60*60*24  #一天秒数
        ONE_MIN = 60
        day = date.date()
        ts = datetime.datetime.strptime(str(day), '%Y-%m-%d').timestamp()
        ts = ts + ONE_DAY - ONE_MIN
        #dt = datetime.datetime.fromtimestamp(ts)
        #print(dt.strftime('%Y-%m-%d %H:%M:%S.%f'))
        ts = int(ts*1000)
        cursor = InfraAPI._get_db_kline_reader(exchange, symbol)
        s, e = await cursor.find_one({'begin_dt':ts})
        if e:
            return None
        return s

    @staticmethod
    async def get_trade_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到trade
        """
        cursor = InfraAPI._get_db_trade_reader(exchange, symbol)
        s, e = await cursor.find_one({'dt':{'$gte':epoch_millisecond,'$lt':epoch_millisecond+tolerance_millisecond+1}})
        if e:
            return None
        return s

    @staticmethod
    async def get_trades_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有trade列表
        """
        cursor = InfraAPI._get_db_trade_reader(exchange, symbol)
        s, e = await cursor.get_list({'dt':{'$gte':begin_epoch_millisecond,'$lt':end_epoch_millisecond}})
        if e:
            return None
        return s

    @staticmethod
    async def get_prev_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个trade
        """
        cursor = InfraAPI._get_db_trade_reader(exchange, symbol)
        sort = [('dt', pymongo.DESCENDING)]
        s, e = await cursor.get_list({'dt':{'$lt':epoch_millisecond}}, sort=sort, limit=n)
        if e:
            return None
        return s

    @staticmethod
    async def get_next_trades(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个trade
        """
        cursor = InfraAPI._get_db_trade_reader(exchange, symbol)
        s, e = await cursor.get_list({'dt':{'$gte':epoch_millisecond}}, limit=n)
        if e:
            return None
        return s

    @staticmethod
    async def get_last_trade_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一笔trade
        """
        ONE_DAY = 60*60*24*1000  #一天毫秒数
        day = date.date()
        ts = datetime.datetime.strptime(str(day), '%Y-%m-%d').timestamp()
        ts = int(ts*1000)
        ts = ts + ONE_DAY - 1
        #dt = datetime.datetime.fromtimestamp(ts)
        #print(dt.strftime('%Y-%m-%d %H:%M:%S.%f'))
        cursor = InfraAPI._get_db_trade_reader(exchange, symbol)
        sort = [('dt', pymongo.DESCENDING)]
        s, e = await cursor.find_one({'dt':{'$lte':ts}}, sort=sort)
        if e:
            return None
        return s

    @staticmethod
    async def get_orderbook_by_time(exchange, symbol, epoch_millisecond, tolerance_millisecond):
        """ 根据给定symbol，给定毫秒时间，容忍毫秒数，找到orderbook
        """
        cursor = InfraAPI._get_db_depth_reader(exchange, symbol)
        s, e = await cursor.find_one({'dt':{'$gte':epoch_millisecond,'$lt':epoch_millisecond+tolerance_millisecond+1}})
        if e:
            return None
        return s

    @staticmethod
    async def get_orderbooks_between(exchange, symbol, begin_epoch_millisecond, end_epoch_millisecond):
        """ 根据给定symbol，给定起始毫秒，结束毫秒，找到所有orderbook列表
        """
        cursor = InfraAPI._get_db_depth_reader(exchange, symbol)
        s, e = await cursor.get_list({'dt':{'$gte':begin_epoch_millisecond,'$lt':end_epoch_millisecond}})
        if e:
            return None
        return s

    @staticmethod
    async def get_prev_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往过去load若干个orderbook
        """
        cursor = InfraAPI._get_db_depth_reader(exchange, symbol)
        sort = [('dt', pymongo.DESCENDING)]
        s, e = await cursor.get_list({'dt':{'$lt':epoch_millisecond}}, sort=sort, limit=n)
        if e:
            return None
        return s

    @staticmethod
    async def get_next_orderbooks(exchange, symbol, epoch_millisecond, n):
        """ 根据当前毫秒数，往未来load若干个orderbook
        """
        cursor = InfraAPI._get_db_depth_reader(exchange, symbol)
        s, e = await cursor.get_list({'dt':{'$gte':epoch_millisecond}}, limit=n)
        if e:
            return None
        return s

    @staticmethod
    async def get_last_orderbook_oneday(exchange, symbol, date):
        """ 给定日期，找到当天的最后一个orderbook
        """
        ONE_DAY = 60*60*24*1000  #一天毫秒数
        day = date.date()
        ts = datetime.datetime.strptime(str(day), '%Y-%m-%d').timestamp()
        ts = int(ts*1000)
        ts = ts + ONE_DAY - 1
        #dt = datetime.datetime.fromtimestamp(ts)
        #print(dt.strftime('%Y-%m-%d %H:%M:%S.%f'))
        cursor = InfraAPI._get_db_depth_reader(exchange, symbol)
        sort = [('dt', pymongo.DESCENDING)]
        s, e = await cursor.find_one({'dt':{'$lte':ts}}, sort=sort)
        if e:
            return None
        return s

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