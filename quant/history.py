# -*- coding:utf-8 -*-

"""
历史行情适配器(同步时间轴)

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import asyncio
import numpy as np
import pandas as pd
from threading import Thread

from quant.config import config
from quant.tasks import SingleTask
from quant.utils import tools, logger
from quant.utils.decorator import async_method_locker


#打印能完整显示
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 50000)
pd.set_option('max_colwidth', 1000)


class HistoryAdapter:
    """ 历史行情适配器(同步时间轴)
    """

    INTERVAL = 1*60*60*1000 #按每小时做为时间间隔读取数据库
    gw_list = []
    current_timestamp = None #回测环境中的"当前时间"
    bind_strategy = None
    
    def __init__(self, **kwargs):
        self.gw_list.append(self)

    @classmethod
    def current_milli_timestamp(cls):
        """ 获取回测环境中的"当前时间"
        """
        return cls.current_timestamp

    @classmethod
    def new_loop_thread(cls, loop):
        #运行事件循环,loop作为参数
        asyncio.set_event_loop(loop)
        loop.run_forever()

    @classmethod
    def initialize(cls, bind_strategy):
        if config.backtest: #回测模式
            cls._start_time = config.backtest["start_time"] #起始时间
            cls._period_day = config.backtest["period_day"] #回测周期
            cls._drive_type = config.backtest["drive_type"] #数据驱动方式:k线驱动,逐笔成交驱动,订单薄驱动
        elif config.datamatrix: #datamatrix模式
            cls._start_time = config.datamatrix["start_time"]
            cls._period_day = config.datamatrix["period_day"]
            cls._drive_type = config.datamatrix["drive_type"]
        #----------------------------------------------------
        ts = tools.datetime_str_to_ts(cls._start_time, fmt='%Y-%m-%d') #转换为时间戳
        ts *= 1000 #转换为毫秒时间戳
        cls.current_timestamp = ts
        cls.bind_strategy = bind_strategy

    @classmethod
    async def start(cls):
        """ 开始喂历史数据
        """
        thread_loop = asyncio.new_event_loop() #创建新的事件循环
        run_loop_thread = Thread(target=cls.new_loop_thread, args=(thread_loop,), name="_work_thread_") #新起线程运行事件循环, 防止阻塞主线程
        run_loop_thread.start() #运行线程，即运行协程事件循环

        #在主线程中运行
        for gw in cls.gw_list:
            await gw.launch() #模拟交易接口连接初始化成功

        #1.算出begin_time和end_time
        #2.然后按1小时为一个单位调用 按drive_type 依次调用gw_list里面每个对象的gw.load_data(drive_type, begin_time, end_time)
        #3.将上一步读取到的所有pandas合并成一个大的pandas, 然后按dt进行排序
        #4.循环遍历这个大的pandas,依据记录里面的self对象，把数据逐条推送给相应BacktestTrader
        #5.BacktestTrader里面将记录按drive_type转换为相应的结构 然后调用相应on_xxxx
        #6.重复第二步
        #备注：每次把时间dt记录下来 作为回测环境的当前时间
        
        begin_time = tools.datetime_str_to_ts(cls._start_time, fmt='%Y-%m-%d') #转换为时间戳
        begin_time *= 1000 #转换为毫秒时间戳
        end_time = begin_time + int(cls._period_day)*24*60*60*1000 #回测结束毫秒时间戳
        
        bt = begin_time
        et = begin_time + cls.INTERVAL
        while et <= end_time: #每次从数据库中读取一段时间的数据
            pd_list = []
            for gw in cls.gw_list:
                for t in cls._drive_type:
                    df = await gw.load_data(t, bt, et)
                    if not df.empty:
                        pd_list.append(df)
            #-------------------------------------
            #设置下一个时间段
            bt = et
            et = bt + cls.INTERVAL
            #-------------------------------------
            #下面的函数一定要上锁,不然当回测的策略面有比如await这样的等待操作的话
            #新的task就会被调度,那样回测数据时间轴就混乱了,所以得上同步锁
            @async_method_locker("HistoryAdapter.start.task") #上锁
            async def task(pd_list):
                if pd_list:
                    #合成一个大表
                    df = pd.concat(pd_list, sort=False)
                    #按采集时间排序,这一步非常关键,它将各种类型历史数据按采集顺序安排在同一个时间轴上,和实盘数据顺序一致
                    sorted_df = df.sort_values(by='dt', kind='mergesort')
                    for _, row in sorted_df.iterrows():
                        cls.current_timestamp = int(row['dt']) #回测环境中的"当前时间"
                        gw = row['gw']
                        await gw.feed(row) #逐行将数据喂给其对应的虚拟适配器接口
                elif pd_list == None:
                    #全部执行完毕,进行收尾工作
                    #通知虚拟适配器
                    for gw in cls.gw_list:
                        await gw.done()
                    #通知策略
                    await cls.bind_strategy.done()
                    #停止之前新建的事件循环线程
                    thread_loop.stop()
            
            #----------------------------------------------------------------
            #实测发现策略回测和读取mongodb数据库(io操作)这两个任务在同一个线程事件loop中并不能实现并发,
            #所以将策略回测任务放到一个新的线程中,这样才能实现并发,不过这样就要注意策略因为是在
            #新线程中一个新的事件循环中运行,没法直接读取数据库(mongodb数据库连接绑定了主线程事件loop),
            #所以需要修改ModelApi,如果是回测模式就需要将数据库操作投递到主线程中的事件loop中执行.
            
            #在主线程中运行
            #SingleTask.run(task, pd_list)
            
            #在工作线程中运行
            asyncio.run_coroutine_threadsafe(task(pd_list), thread_loop)
        #end while
        
        #完成通知
        asyncio.run_coroutine_threadsafe(task(None), thread_loop)