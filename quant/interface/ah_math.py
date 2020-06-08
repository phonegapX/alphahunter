# -*- coding:utf-8 -*-

"""
AHMath module.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import copy
import collections
import warnings
import math
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import norm


class AHMath(object):
    """ alphahunter 常用数学函数
    """

    @staticmethod
    def array(num_list):
        """ list类型转换成numpy array类型
        """
        return np.array(num_list)

    @staticmethod
    def multiply(a, b):
        """ 返回两个数的乘积，出现任何异常，返回None
        """
        if pd.isnull(a) or pd.isnull(b):
            return None
        else:
            return a * b

    @staticmethod
    def power(a, b):
        """ a的b次方
        """
        return math.pow(a, b)

    @staticmethod
    def exp(a):
        """ e的a次方
        """
        return math.exp(a)
    
    @staticmethod
    def expm1(a):
        """ e的a次方减1
        """
        return math.expm1(a)
    
    @staticmethod
    def log(a):
        """ e为底的log(a)
        """
        return math.log(a)
    
    @staticmethod
    def log1p(a):
        """ log(1 + a)
        """
        return math.log1p(a)
    
    @staticmethod
    def sqrt(a):
        """ a的平方根
        """
        return math.sqrt(a)
    
    @staticmethod
    def abs(a):
        """ a的绝对值
        """
        return math.fabs(a)
    
    @staticmethod
    def copysign(a, b):
        """ b的正负号乘以a
        """
        return math.copysign(a, b)

    @staticmethod
    def zeros(a):
        """ 长度为a，元素都为0的numpy array类型
        """
        return np.zeros(a)
    
    @staticmethod
    def ones(a):
        """ 长度为a的，元素都为1的numpy array类型
        """
        return np.ones(a)

    @staticmethod
    def max(a):
        """ 返回一个列表里面最大的元素，出现任何异常，返回None
        """
        if (a is None) or (len(a) == 0):
            return None
        a_array = np.array([i for i in a if pd.notnull(i)])
        count = len(a_array)
        if count == 0:
            return None
        else:
            return a_array.max()

    @staticmethod
    def min(a):
        """ 返回一个列表里面最小的元素，出现任何异常，返回None
        """
        if (a is None) or (len(a) == 0):
            return None
        a_array = np.array([i for i in a if pd.notnull(i)])
        count = len(a_array)
        if count == 0:
            return None
        else:
            return a_array.min()

    @staticmethod
    def sum(a):
        """ 返回一个列表里面所有元素的和，出现任何异常，返回0.0
        """
        if (a is None) or (len(a) == 0):
            return 0.0
        result = 0.0 if pd.isnull(a[0]) else a[0]
        for i in range(1, len(a)):
            if pd.isnull(a[i]):
                continue
            result += a[i]
        return result

    @staticmethod
    def cum_sum(a):
        """ 返回一个list的累积求和列表类型，如果其中有None值，按照0.0处理
        """
        if (a is None) or (len(a) == 0):
            return [0.0]
        b = [each if pd.notnull(each) else 0.0 for each in a]
        return list(np.array(b).cumsum())

    @staticmethod
    def dot(a, b):
        """ 返回两个列表的点乘乘积，出现异常，返回None
        """
        if len(a) != len(b):
            return None
        else:
            a = AHMath.array(a)
            b = AHMath.array(b)
            a_m_b = AHMath.array([AHMath.multiply(a[i], b[i]) for i in range(len(a))])
            return AHMath.sum(a_m_b)

    @staticmethod
    def count_nan(a):
        """ 返回一个列表里None值的个数，出现异常，返回None
        """
        count = 0
        if a is None:
            return None
        for i in a:
            if pd.isnull(i):
                count += 1
        return count

    @staticmethod
    def mean(a):
        """ 返回一个列表里面元素的平均值，出现任何异常，返回None
        """
        if (a is None) or (len(a) == 0):
            return None
        count = len(a) - AHMath.count_nan(a)
        if count == 0:
            return None
        return AHMath.sum(a) / float(count)

    @staticmethod
    def std(a):
        """ 返回一个列表里面元素的标准差，出现任何异常，返回None
        """
        if (a is None) or (len(a) == 0):
            return None
        count = len(a) - AHMath.count_nan(a)
        if count <= 1:
            return None
        mean = AHMath.mean(a)
        s = 0
        for e in a:
            if pd.isnull(e):
                continue
            s += (e - mean) ** 2
        return AHMath.sqrt(s / float(count - 1))

    @staticmethod
    def weighted_mean(a, w):
        """ 给定一个列表w作为权重，返回另一个列表a的加权平均值，出现任何异常，返回None
        """
        if len(a) != len(w):
            print('weighted mean lists not same length')
            return None
        s = 0
        w_sum = 0
        for i in range(0, len(a)):
            if (pd.isnull(a[i])) or (pd.isnull(w[i])):
                continue
            s += a[i] * w[i]
            w_sum += w[i]
        if w_sum == 0:
            print('sum of weight is 0, can not divided by 0')
            return None
        return s / float(w_sum)

    @staticmethod
    def sma(a, n):
        """ 返回一个列表a最末n个元素的简单平均值，出现任何异常，返回None
        """
        if len(a) < n:
            print('list lenght less than requirement: ', n)
            return None
        return AHMath.mean(a[-n:])

    # decay index alpha, 0:0.5, 1:0.1, 2:0.05, 3:0.02, 4:0.01, 5:0.005, 6:0.001, 7:0.95, 8:0.9
    @staticmethod
    def ema_alpha(a, n, alpha):
        """ 返回一个列表a里面最末n个元素的指数平均值，衰减参数是alpha，出现任何异常，返回None
        """
        if len(a) < n:
            print('list length less than requirement: ', n)
        count = len(a) - AHMath.count_nan(a)
        if count == 0:
            print('all values nan')
            return None
        result = 0.0
        for i in a:
            if pd.notnull(i):
                result = alpha * i + (1 - alpha) * result
        return result

    @staticmethod
    def wma(a, w, n):
        """ 给定一个列表w作为权重，返回另一个列表a里面最末n个元素的加权平均值，出现任何异常，返回None
        """
        if len(a) < n or len(w) < n:
            print('lists length less than requirement: ', n)
            return None
        return AHMath.weighted_mean(a[-n:], w[-n:])

    @staticmethod
    def ls_regression(x, y, add_constant = True):
        """ 给定列表x，y，返回线性回归对象，默认带常数项
        """
        if add_constant:
            return sm.OLS(y, sm.add_constant(x)).fit()
        else:
            return sm.OLS(y, x).fit()

    @staticmethod
    def wls_regression(x, y, w, add_constant = True):
        """ 给定列表x，y，给定列表w作为权重，返回带权重线性回归对象，默认带常数项
        """
        if add_constant:
            return sm.WLS(y, sm.add_constant(x), weights = w).fit()
        else:
            return sm.WLS(y, x, weights = w).fit()

    @staticmethod
    def reg_const(reg):
        """ 返回线性回归对象的常数项
        """
        return reg.params[0]

    @staticmethod
    def reg_betas(reg):
        """ 返回线性回归对象的变量所对应的系数
        """
        return reg.params[1:]

    @staticmethod
    def r_squared(reg):
        """ 返回线性回归对象的r平方
        """
        return reg.rsquared

    @staticmethod
    def r_squared_adj(reg):
        """ 返回线性回归对象的调整后r平方
        """
        return reg.rsquared_adj

    @staticmethod
    def reg_const_tstats(reg):
        """ 返回线性回归对象的常数项t统计量
        """
        return reg.tvalues[0]

    @staticmethod
    def reg_beta_tstats(reg):
        """ 返回线性回归对象的非常数项系数的t统计量
        """
        return reg.tvalues[1:]

    @staticmethod
    def corr(x, y):
        """ 返回两个列表的相关系数，出现异常，返回None
        """
        if (x is None) or (y is None):
            print('x or y is None')
            return None
        if len(x) != len(y):
            print('x has not the same length as y')
            return None
        if len(x) < 2:
            print('x is too short')
            return None
        x_mean = AHMath.mean(x)
        y_mean = AHMath.mean(y)
        xy = 0.0
        x2 = 0.0
        y2 = 0.0
        count = 0
        for i in range(len(x)):
            if pd.notnull(x[i]) and pd.notnull(y[i]):
                count += 1
                xy += (x[i] - x_mean) * (y[i] - y_mean)
                x2 += (x[i] - x_mean) ** 2
                y2 += (y[i] - y_mean) ** 2
        x_std = (x2 / float(count - 1)) ** 0.5
        y_std = (y2 / float(count - 1)) ** 0.5
        xy_mean = xy / float(count)
        corr = xy_mean / float(x_std) / float(y_std) if x_std > 0 and y_std > 0 else 0.0
        return corr

    @staticmethod
    def linear_rank(rank_dict, begin=-0.5, end=0.5, reverse_value=False):
        """ 对原字典value做线性排序，从-0.5至0.5，返回字典对象
        """
        asc = not reverse_value
        key, value = tuple(zip(*rank_dict.items()))
        rank = pd.Series(value).rank(na_option='keep', ascending=asc)
        max_value = rank.count()
        rank = np.where(np.isnan(rank), None, (rank - 1) * (end - begin) / float(max_value - 1) + begin)
        return dict(zip(key, rank))

    @staticmethod
    def normal_rank(rank_dict):
        """ 对原字典value做正态排序，减去均值，除以标准差，返回字典对象
        """
        od = copy.copy(rank_dict)
        count = len(od) - AHMath.count_nan(od.values())
        mean = 0
        for key in od.keys():
            if pd.isnull(od[key]):
                continue
            mean += od[key]
        mean = mean / float(count)
        std = 0
        for key in od.keys():
            if pd.isnull(od[key]):
                continue
            std += (od[key] - mean) ** 2
        std = math.sqrt(std / float(count - 1))
        for key in od.keys():
            if std == 0:
                od[key] = 0
            else:
                od[key] = (od[key] - mean) / float(std)
        return od

    @staticmethod
    def linear_normal_rank(rank_dict, reverse_value=False):
        """ 对原字典value做线性正态排序，找到自然数顺序排序，并根据排序值返回正态分布
        累计概率分布的反函数所对应的值，返回字典对象
        """
        od = collections.OrderedDict(sorted(rank_dict.items(), key=lambda t: t[1], reverse=reverse_value))
        rank_num = len(od) - AHMath.count_nan(od.values())
        i = 0
        for key in od.keys():
            if pd.isnull(od[key]):
                continue
            od[key] = norm.ppf(float(i + 1) / (rank_num + 1))
            i += 1
        return od

    @staticmethod
    def zero_divide(x, y):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = np.divide(x, y)
        if hasattr(y, "__len__"):
            res[y == 0] = 0
        elif y == 0:
            res = 0
        return res

    @staticmethod
    def ewma(x, halflife, init=0, min_periods=0, ignore_na=False, adjust=False):
        init_s = pd.Series(data=init)
        s = init_s.append(x)
        if adjust:
            xx = range(len(x))
            lamb = 1 - 0.5**(1/halflife)
            adjfactor = 1-np.power(1-lamb, xx)*(1-lamb)
            r = s.ewm(halflife=halflife, min_periods=min_periods, ignore_na=ignore_na, adjust=False).mean().iloc[1:]
            return r/adjfactor
        else:
            return s.ewm(halflife=halflife, min_periods=min_periods, ignore_na=ignore_na, adjust=False).mean().iloc[1:]