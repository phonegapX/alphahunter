# -*- coding:utf-8 -*-

"""
okex行情采集模块

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.collect import Collect
from quant.startup import default_main


if __name__ == '__main__':
    default_main(Collect)
