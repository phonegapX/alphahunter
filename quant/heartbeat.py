# -*- coding:utf-8 -*-

"""
服务器心跳

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import asyncio

from quant.utils import tools
from quant.utils import logger
from quant.config import config

__all__ = ("heartbeat", )


class HeartBeat(object):
    """ 心跳
    """

    def __init__(self):
        self._count = 0  # 心跳次数
        self._interval = 1  # 服务心跳执行时间间隔(秒)
        self._print_interval = config.heartbeat.get("interval", 0)  # 心跳打印时间间隔(秒)，0为不打印
        self._broadcast_interval = config.heartbeat.get("broadcast", 0)  # 心跳广播间隔(秒)，0为不广播
        self._tasks = {}  # 跟随心跳执行的回调任务列表，由 self.register 注册 {task_id: {...}}

    @property
    def count(self):
        return self._count

    def ticker(self):
        """ 启动心跳， 每秒执行一次
        """
        self._count += 1

        # 打印心跳次数
        if self._print_interval > 0:
            if self._count % self._print_interval == 0:
                logger.info("do server heartbeat, count:", self._count, caller=self)

        # 设置下一次心跳回调
        asyncio.get_event_loop().call_later(self._interval, self.ticker)

        # 执行任务回调
        for task_id, task in self._tasks.items():
            interval = task["interval"]
            if self._count % interval != 0:
                continue
            func = task["func"]
            args = task["args"]
            kwargs = task["kwargs"]
            kwargs["task_id"] = task_id
            kwargs["heart_beat_count"] = self._count
            asyncio.get_event_loop().create_task(func(*args, **kwargs))

        # 广播服务进程心跳
        if self._broadcast_interval > 0:
            if self._count % self._broadcast_interval == 0:
                self.alive()

    def register(self, func, interval=1, *args, **kwargs):
        """ 注册一个任务，在每次心跳的时候执行调用
        @param func 心跳的时候执行的函数
        @param interval 执行回调的时间间隔(秒)
        @return task_id 任务id
        """
        t = {
            "func": func,
            "interval": interval,
            "args": args,
            "kwargs": kwargs
        }
        task_id = tools.get_uuid1()
        self._tasks[task_id] = t
        return task_id

    def unregister(self, task_id):
        """ 注销一个任务
        @param task_id 任务id
        """
        if task_id in self._tasks:
            self._tasks.pop(task_id)

    def alive(self):
        """ 服务进程广播心跳
        """
        from quant.event import EventHeartbeat
        EventHeartbeat(config.server_id, self.count).publish()


heartbeat = HeartBeat()
