# -*- coding:utf-8 -*-

"""
虚拟交易所,用于DataMatrix,可参考文档中的DataMatrix架构图

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import os
import csv
from quant.utils import logger
from quant.order import ORDER_TYPE_LIMIT
from quant.history import VirtualTrader


__all__ = ("DataMatrixTrader",)


class DataMatrixTrader(VirtualTrader):
    """ DataMatrixTrader module.
    """

    def __init__(self, **kwargs):
        """Initialize."""
        super(DataMatrixTrader, self).__init__(**kwargs)
        self._csv_write = None
        self._f = None

    def csv_write(self, header, row):
        """ 写csv文件
        """
        if not self._csv_write: #如果csv文件还没初始化,就初始化它
            csv_file = os.path.dirname(os.path.abspath(sys.argv[0])) + "/output.csv"
            if os.path.isdir(csv_file) or os.path.ismount(csv_file) or os.path.islink(csv_file):
                logger.error("无效的csv文件")
                return
            if os.path.isfile(csv_file):
                os.remove(csv_file)
            self._f = open(csv_file, 'a+', newline='')
            self._csv_write = csv.DictWriter(self._f, fieldnames=header)
            self._csv_write.writeheader()
        #写一行内容
        self._csv_write.writerow(row)

    async def done(self):
        """ DataMatrix完成
        """
        self._f.close()

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, *args, **kwargs):
        """ Create an order.

        Args:
            symbol: Trade target
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        raise NotImplementedError #datamatrix模块不需要此功能

    async def revoke_order(self, symbol, *order_nos):
        """ Revoke (an) order(s).

        Args:
            symbol: Trade target
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel all orders for 
            this symbol. If you set 1 or multiple param, you can cancel an or multiple order.

        Returns:
            删除全部订单情况: 成功=(True, None), 失败=(False, error information)
            删除单个或多个订单情况: (删除成功的订单id[], 删除失败的订单id及错误信息[]),比如删除三个都成功那么结果为([1xx,2xx,3xx], [])
        """
        raise NotImplementedError #datamatrix模块不需要此功能

    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        raise NotImplementedError #datamatrix模块不需要此功能

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        raise NotImplementedError #datamatrix模块不需要此功能

    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        raise NotImplementedError #datamatrix模块不需要此功能

    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        raise NotImplementedError #datamatrix模块不需要此功能

    async def invalid_indicate(self, symbol, indicate_type):
        """ update (an) callback function.

        Args:
            symbol: Trade target
            indicate_type: INDICATE_ORDER, INDICATE_ASSET, INDICATE_POSITION

        Returns:
            success: If execute successfully, return True, otherwise it's False.
            error: If execute failed, return error information, otherwise it's None.
        """
        raise NotImplementedError #datamatrix模块不需要此功能

    @staticmethod
    def mapping_layer():
        """ 获取符号映射关系.
        Returns:
            layer: 符号映射关系
        """
        return None #DataMatrix模块不需要符号映射