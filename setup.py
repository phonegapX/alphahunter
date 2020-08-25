# -*- coding:utf-8 -*-

#from distutils.core import setup
from setuptools import setup

setup(
    name="alphahunter",
    version="0.9.1",
    packages=[
        "quant",
        "quant.utils",
        "quant.platform",
    ],
    description="Asynchronous driven quantitative trading framework.",
    url="https://github.com/phonegapx/alphahunter",
    author="HJQuant",
    author_email="8342537@qq.com",
    license="MIT",
    keywords=[
        "alphahunter", "quant", "framework", "async", "asynchronous", "digiccy", "digital", "currency",
        "marketmaker", "binance", "okex", "huobi", "bitmex", "ftx"
    ],
    install_requires=[
        "aiohttp==3.2.1",
        "aioamqp==0.13.0",
        "motor==2.0.0"
    ],
)
