# -*- coding:utf-8 -*-

"""
错误信息

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


class Error:

    def __init__(self, msg):
        self._msg = msg

    @property
    def msg(self):
        return self._msg

    def __str__(self):
        return str(self._msg)

    def __repr__(self):
        return str(self)
