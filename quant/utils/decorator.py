# -*- coding:utf-8 -*-

"""
Decorator.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import asyncio
import functools


# Coroutine lockers. e.g. {"locker_name": locker}
METHOD_LOCKERS = {}


def async_method_locker(name, wait=True):
    """ In order to share memory between any asynchronous coroutine methods, we should use locker to lock our method,
        so that we can avoid some un-prediction actions.

    Args:
        name: Locker name.
        wait: If waiting to be executed when the locker is locked? if True, waiting until to be executed, else return
            immediately (do not execute).

    NOTE:
        This decorator must to be used on `async method`.
    """
    assert isinstance(name, str)

    def decorating_function(method):
        global METHOD_LOCKERS
        locker = METHOD_LOCKERS.get(name)
        if not locker:
            locker = asyncio.Lock()
            METHOD_LOCKERS[name] = locker

        @functools.wraps(method)
        async def wrapper(*args, **kwargs):
            if not wait and locker.locked():
                return
            try:
                await locker.acquire()
                return await method(*args, **kwargs)
            finally:
                locker.release()
        return wrapper
    return decorating_function


# class Test:
#
#     @async_method_locker('my_fucker', False)
#     async def test(self, x):
#         print('hahaha ...', x)
#         await asyncio.sleep(0.1)
#
#
# t = Test()
# for i in range(10):
#     asyncio.get_event_loop().create_task(t.test(i))
#
# asyncio.get_event_loop().run_forever()
