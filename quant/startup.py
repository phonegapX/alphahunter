# -*- coding:utf-8 -*-

"""
启动模块

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import os

from quant.utils import logger


def default_main(strategy_class):
    """ 默认启动函数
    """
    config_file = os.path.abspath("config.json")
    if not os.path.isfile(config_file):
        logger.error("config.json miss")
        return
    from quant.quant import quant
    quant.initialize(config_file)
    strategy_class()
    quant.start()


def command_main(strategy_class):
    """ 配置文件通过参数传入的启动函数
    """
    if len(sys.argv) <= 1:
        logger.error("config file miss")
        return
    config_file = sys.argv[1]
    if not config_file.lower().endswith(".json"):
        logger.error("must xxx.json file")
        return
    if not os.path.isfile(config_file):
        logger.error("config file miss")
        return
    from quant.quant import quant
    quant.initialize(config_file)
    strategy_class()
    quant.start()