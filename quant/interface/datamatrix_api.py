# -*- coding:utf-8 -*-

"""
datamatrix_api

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.interface.model_api import ModelAPI


class DataMatrixAPI(ModelAPI):
    """ DataMatrixAPI, 继承ModelAPI的所有功能
    """

    def __init__(self):
        """ 初始化
        """
        super(DataMatrixAPI, self).__init__()