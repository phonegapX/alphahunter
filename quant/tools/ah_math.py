# -*- coding:utf-8 -*-

"""
AHMath module.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import math
import pandas as pd
import numpy as np
import statsmodels.api as sm
import collections
import copy
from scipy.stats import norm


class AHMath(object):

    @staticmethod
    def array(num_list):
        return np.array(num_list)

    @staticmethod
    def multiply(a, b):
        if pd.isnull(a) or pd.isnull(b):
            return None
        else:
            return a * b

    @staticmethod
    def power(a, b):
        return math.pow(a, b)

    @staticmethod
    def exp(a):
        return math.exp(a)
    
    @staticmethod
    def expm1(a):
        return math.expm1(a)
    
    @staticmethod
    def log(a):
        return math.log(a)
    
    @staticmethod
    def log1p(a):
        return math.log1p(a)
    
    @staticmethod
    def sqrt(a):
        return math.sqrt(a)
    
    @staticmethod
    def abs(a):
        return math.fabs(a)
    
    @staticmethod
    def copysign(a, b):
        return math.copysign(a, b)

    @staticmethod
    def zeros(a):
        return np.zeros(a)
    
    @staticmethod
    def ones(a):
        return np.ones(a)

    @staticmethod
    def max(a):
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
        if (a is None) or (len(a) == 0):
            return 0.0
        b = [each if pd.notnull(each) else 0.0 for each in a]
        return list(np.array(b).cumsum())

    @staticmethod
    def dot(a, b):
        if len(a) != len(b):
            return None
        else:
            a = AHMath.array(a)
            b = AHMath.array(b)
            a_m_b = AHMath.array([AHMath.multiply(a[i], b[i]) for i in range(len(a))])
            return AHMath.sum(a_m_b)

    @staticmethod
    def count_nan(a):
        count = 0
        if (a is None) or (len(a) == 0):
            return count
        for i in a:
            if pd.isnull(i):
                count += 1
        return count

    @staticmethod
    def mean_test(a):
        return np.mean(a)

    @staticmethod
    def mean(a):
        if (a is None) or (len(a) == 0):
            return None
        count = len(a) - AHMath.count_nan(a)
        if count == 0:
            return None
        return AHMath.sum(a) / float(count)

    @staticmethod
    def std(a):
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
    def sma(self, a, n):
        if len(a) < n:
            print('list lenght less than requirement: ', n)
            return None
        return self.mean(a[-n:])

    # decay index alpha, 0:0.5, 1:0.1, 2:0.05, 3:0.02, 4:0.01, 5:0.005, 6:0.001, 7:0.95, 8:0.9
    @staticmethod
    def ema_alpha(self, a, n, alpha):
        if len(a) < n:
            print('list length less than requirement: ', n)
        count = len(a) - self.count_nan(a)
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
        if len(a) < n or len(w) < n:
            print('lists length less than requirement: ', n)
            return None
        return AHMath.weighted_mean(a[-n:], w[-n:])

    @staticmethod
    def ls_regression(x, y, add_constant = True):
        if add_constant:
            return sm.OLS(y, sm.add_constant(x)).fit()
        else:
            return sm.OLS(y, x).fit()

    @staticmethod
    def wls_regression(x, y, w, add_constant = True):
        if add_constant:
            return sm.WLS(y, sm.add_constant(x), weights = w).fit()
        else:
            return sm.WLS(y, x, weights = w).fit()

    @staticmethod
    def reg_const(reg):
        return reg.params[0]
    
    @staticmethod
    def reg_betas(reg):
        return reg.params[1:]
    
    @staticmethod
    def r_squared(reg):
        return reg.rsquared
    
    @staticmethod
    def r_squared_adj(reg):
        return reg.rsquared_adj
    
    @staticmethod
    def reg_const_tstats(reg):
        return reg.tvalues[0]
    
    @staticmethod
    def reg_beta_tstats(reg):
        return reg.tvalues[1:]

    @staticmethod
    def corr(x, y):
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
        asc = not reverse_value
        key, value = tuple(zip(*rank_dict.items()))
        rank = pd.Series(value).rank(na_option='keep', ascending=asc)
        max_value = rank.count()
        rank = np.where(np.isnan(rank), None, (rank - 1) * (end - begin) / float(max_value - 1) + begin)
        return dict(zip(key, rank))

    @staticmethod
    def normal_rank(rank_dict):
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
        od = collections.OrderedDict(sorted(rank_dict.items(), key=lambda t: t[1], reverse=reverse_value))
        rank_num = len(od) - AHMath.count_nan(od.values())
        i = 0
        for key in od.keys():
            if pd.isnull(od[key]):
                continue
            od[key] = norm.ppf(float(i + 1) / (rank_num + 1))
            i += 1
        return od
