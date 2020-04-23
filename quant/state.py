# -*- coding:utf-8 -*-

"""
状态信息

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


class State:

    STATE_CODE_PARAM_MISS = 1       #交易接口初始化过程缺少参数
    STATE_CODE_CONNECT_SUCCESS = 2  #交易接口连接成功
    STATE_CODE_CONNECT_FAILED = 3   #交易接口连接失败
    STATE_CODE_DISCONNECT = 4       #交易接口连接断开
    STATE_CODE_RECONNECTING = 5     #交易接口重新连接中
    STATE_CODE_READY = 6            #交易接口准备好
    STATE_CODE_GENERAL_ERROR = 7    #交易接口常规错误
    STATE_CODE_DB_SUCCESS = 8       #数据库连接成功
    STATE_CODE_DB_ERROR = 9         #数据库连接失败

    def __init__(self, platform, account, msg, code = STATE_CODE_PARAM_MISS):
        self._platform = platform
        self._account = account
        self._msg = msg
        self._code = code

    @property
    def platform(self):
        return self._platform
    
    @property
    def account(self):
        return self._account

    @property
    def msg(self):
        return self._msg
    
    @property
    def code(self):
        return self._code

    def __str__(self):
        return "platform:{} account:{} msg:{}".format(self._platform, self._account, self._msg)

    def __repr__(self):
        return str(self)
