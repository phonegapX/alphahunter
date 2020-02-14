# -*- coding:utf-8 -*-

"""
状态信息

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


class State:

    STATE_CODE_PARAM_MISS = 1       #参数丢失
    STATE_CODE_CONNECT_SUCCESS = 2  #连接成功
    STATE_CODE_CONNECT_FAILED = 3   #连接失败
    STATE_CODE_DISCONNECT = 4       #连接断开
    STATE_CODE_RECONNECTING = 5     #重新连接中
    STATE_CODE_READY = 6            #策略环境准备好
    STATE_CODE_GENERAL_ERROR = 7    #一般常规错误

    def __init__(self, msg, code = STATE_CODE_PARAM_MISS):
        self._msg = msg
        self._code = code

    @property
    def msg(self):
        return self._msg
    
    @property
    def code(self):
        return self._code

    def __str__(self):
        return str(self._msg)

    def __repr__(self):
        return str(self)
