# -*- coding:utf-8 -*-

"""
日志打印

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import os
import sys
import shutil
import logging
import traceback
from logging.handlers import TimedRotatingFileHandler

initialized = False


def initLogger(log_level="DEBUG", log_path=None, logfile_name=None, clear=False, backup_count=0):
    """ 初始化日志输出
    @param log_level 日志级别 DEBUG/INFO
    @param log_path 日志输出路径
    @param logfile_name 日志文件名
    @param clear 初始化的时候，是否清理之前的日志文件
    @param backup_count 保存按天分割的日志文件个数，默认0为永久保存所有日志文件
    """
    global initialized
    if initialized:
        return
    logger = logging.getLogger()
    logger.setLevel(log_level)
    if logfile_name:
        if clear and os.path.isdir(log_path):
            shutil.rmtree(log_path)
        if not os.path.isdir(log_path):
            os.makedirs(log_path)
        logfile = os.path.join(log_path, logfile_name)
        handler = TimedRotatingFileHandler(logfile, "midnight", backupCount=backup_count)
        print("init logger ...", logfile)
    else:
        print("init logger ...")
        handler = logging.StreamHandler()
    fmt_str = "%(levelname)1.1s [%(asctime)s] %(message)s"
    fmt = logging.Formatter(fmt=fmt_str, datefmt=None)
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    initialized = True


def info(*args, **kwargs):
    func_name, kwargs = _log_msg_header(*args, **kwargs)
    logging.info(_log(func_name, *args, **kwargs))


def warn(*args, **kwargs):
    msg_header, kwargs = _log_msg_header(*args, **kwargs)
    logging.warning(_log(msg_header, *args, **kwargs))


def debug(*args, **kwargs):
    msg_header, kwargs = _log_msg_header(*args, **kwargs)
    logging.debug(_log(msg_header, *args, **kwargs))


def error(*args, **kwargs):
    logging.error("*" * 60)
    msg_header, kwargs = _log_msg_header(*args, **kwargs)
    logging.error(_log(msg_header, *args, **kwargs))
    logging.error("*" * 60)


def exception(*args, **kwargs):
    logging.error("*" * 60)
    msg_header, kwargs = _log_msg_header(*args, **kwargs)
    logging.error(_log(msg_header, *args, **kwargs))
    # exc_info = sys.exc_info()
    # traceback.print_exception(*exc_info)
    logging.error(traceback.format_exc())
    logging.error("*" * 60)


def _log(msg_header, *args, **kwargs):
    _log_msg = msg_header
    for l in args:
        if type(l) == tuple:
            ps = str(l)
        else:
            try:
                ps = "%r" % l
            except:
                ps = str(l)
        if type(l) == str:
            _log_msg += ps[1:-1] + " "
        else:
            _log_msg += ps + " "
    if len(kwargs) > 0:
        _log_msg += str(kwargs)
    return _log_msg


def _log_msg_header(*args, **kwargs):
    """ 打印日志的message头
    @param kwargs["caller"] 调用的方法所属类对象
    * NOTE: logger.xxx(... caller=self) for instance method
            logger.xxx(... caller=cls) for @classmethod
    """
    cls_name = ""
    func_name = sys._getframe().f_back.f_back.f_code.co_name
    session_id = "-"
    try:
        _caller = kwargs.get("caller", None)
        if _caller:
            if not hasattr(_caller, "__name__"):
                _caller = _caller.__class__
            cls_name = _caller.__name__
            del kwargs["caller"]
    except:
        pass
    finally:
        msg_header = "[{session_id}] [{cls_name}.{func_name}] ".format(cls_name=cls_name, func_name=func_name,
                                                                       session_id=session_id)
        return msg_header, kwargs
