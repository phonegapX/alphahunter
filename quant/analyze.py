# -*- coding:utf-8 -*-

"""
生成回测报告

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import os
import codecs
import json
import numpy as np
import pandas as pd

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import Formatter

from quant.utils import tools, logger
from quant.config import config
from quant.interface.model_api import ModelAPI
from quant.report import Report
from quant import SOURCE_ROOT_DIR

STATIC_FOLDER = os.path.abspath(os.path.join(SOURCE_ROOT_DIR, "static")) #模板文件

TO_PCT = 100.0

ONE_DAY = 24*60*60*1000
CHINA_TZONE_SHIFT = 8*60*60*1000

MPL_RCPARAMS = {'figure.facecolor': '#F6F6F6',
                'axes.facecolor': '#F6F6F6',
                'axes.edgecolor': '#D3D3D3',
                'text.color': '#555555',
                'grid.color': '#B1B1B1',
                'grid.alpha': 0.3,
                # scale
                'axes.linewidth': 2.0,
                'axes.titlepad': 12,
                'grid.linewidth': 1.0,
                'grid.linestyle': '-',
                # font size
                'font.size': 13,
                'axes.titlesize': 18,
                'axes.labelsize': 14,
                'legend.fontsize': 'small',
                'lines.linewidth': 2.5}

#打印能完整显示
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 50000)
pd.set_option('max_colwidth', 1000)


class MyFormatter(Formatter):
    def __init__(self, dates, fmt='%Y%m'):
        self.dates = dates
        self.fmt = fmt

    def __call__(self, x, pos=0):
        """Return the label for time x at position pos"""
        ind = int(np.round(x))
        if ind >= len(self.dates) or ind < 0:
            return ''
        # return self.dates[ind].strftime(self.fmt)
        return pd.to_datetime(self.dates[ind]/1000, unit='s').strftime(self.fmt)

class Analyzer(object):
    """ 回测结果分析,生成回测报告
    """

    def __init__(self):
        self.universe = [] #交易符号列表
        self.closes = None #每日收盘价
        self.trades = None #逐笔成交相关
        self.daily = None  #每日成交统计相关
        self.returns = None #日收益率相关
        self.df_pnl = None #日盈亏
        self.daily_dic = dict() #用于报告输出
        self.performance_metrics = dict() #业绩指标
        self.risk_metrics = dict() #风险指标
        self.report_dic = dict() #最终报告

    async def get_daily_closes(self):
        """ 获取每日收盘价
        """
        start_date = tools.datetime_str_to_ts(config.backtest["start_time"], fmt='%Y-%m-%d') #转换为时间戳
        start_date *= 1000 #转换为毫秒时间戳
        end_date = start_date + self.period_day*ONE_DAY #回测结束毫秒时间戳
        
        pd_list = []
        
        for x in config.platforms:
            platform = x["platform"]
            for sym in x["symbols"]:
                r = await ModelAPI.get_klines_between(platform, sym, start_date, end_date)
                if not r: #获取行情失败
                    return None
                df = pd.DataFrame()
                df_temp = pd.DataFrame(r)
                df['end_dt'] = df_temp['end_dt']
                df['close'] = df_temp['close_avg_fillna']
                df['platform'] = platform
                df['symbol'] = sym
                pd_list.append(df)

        df = pd.concat(pd_list)
        df['trade_date'] = (df['end_dt']+CHINA_TZONE_SHIFT)//ONE_DAY*ONE_DAY-CHINA_TZONE_SHIFT
        df = df.set_index(['platform', 'symbol', 'trade_date']).sort_index(axis=0)
        df = df.drop(['end_dt'], axis=1)

        def _get_last(ser):
            r = 0
            if ser.count() > 0:
                for i in range(1, ser.count()):
                    r = ser.iat[-i]
                    if r > 0:
                        break
            return r

        gp = df.reset_index().groupby(by=['platform', 'symbol', 'trade_date'])
        df = gp.agg({'close': _get_last}) #因为获取的是分钟级别收盘价,所以要合成为日级别的收盘价
        return df

    async def initialize(self, file_folder='.'):
        """
        """
        type_map = {'platform': str,
                    'account': str,
                    'symbol': str,
                    'strategy': str,
                    'order_no': str,
                    'fill_no': str,
                    'price': float,
                    'quantity': float,
                    'side': str,
                    'liquidity': str,
                    'fee': float,
                    'ctime': np.int64}

        trades = pd.read_csv(os.path.join(file_folder, 'trades.csv'), ',', dtype=type_map)
        if trades.empty:
            logger.error("error:", "无法读取成交列表", caller=self)
            return False
        #因为我们时间用的都是东八区时间,换成时间戳以后并不是按天对齐的,为了能够按天对齐进行运算,要先加八小时,再减八小时
        trades['trade_date'] = (trades['ctime']+CHINA_TZONE_SHIFT)//ONE_DAY*ONE_DAY-CHINA_TZONE_SHIFT
        self.trades = trades.set_index(['platform', 'symbol', 'ctime']).sort_index(axis=0)
        #保存交易符号列表
        self.trades.groupby(by=['platform', 'symbol']).apply(lambda gp_df: self.universe.append((gp_df.index.levels[0][0], gp_df.index.levels[1][0])))
        #回测天数
        self.period_day = int(config.backtest["period_day"])
        #获取每日收盘价
        closes = await self.get_daily_closes()
        if closes is None:
            logger.error("error:", "无法获取每日收盘价", caller=self)
            return False
        self.closes = closes
        #
        return True

    def process_trades(self):
        """ 处理逐笔成交列表,计算并添加新的字段
        """
        df = self.trades
        # pre-process
        cols_to_drop = ['account', 'strategy', 'order_no', 'fill_no', 'liquidity']
        df = df.drop(cols_to_drop, axis=1) #抛弃不用的列
        #
        def _apply(gp_df):
            direction = gp_df['side'].apply(lambda s: 1 if s=="BUY" else -1)
            price, quantity = gp_df['price'], gp_df['quantity']
            platform = gp_df.index.levels[0][0]
            symbol = gp_df.index.levels[1][0]
            syminfo = config.backtest["feature"][platform]["syminfo"][symbol]
            is_spot = False #是否是现货
            is_future = False #是否是期货
            is_inverse = False #如果是期货的话,是否是反向合约
            if syminfo["type"] == "spot": #现货
                is_spot = True
            elif syminfo["type"] == "future": #期货
                is_future = True
                is_inverse = syminfo["is_inverse"]
            #----------------------------------------
            mult = 1 #合约乘数默认1
            if is_spot: #现货
                turnover = quantity * price #成交额
                #现货手续费买入扣'货',卖出扣'钱',所以要统一成'钱'
                #方法一:向量运算版
                fee = gp_df['fee']
                mask = direction==-1
                commission = (direction + 1) / 2 * fee * price
                commission[mask] = fee[mask]
                gp_df['commission'] = commission
                #方法二:标量循环版
                #gp_df['commission'] = gp_df[['fee', 'side', 'price']].apply(lambda s: (s['fee']*s['price']) if s['side']=="BUY" else s['fee'], axis=1)
            elif is_future: #期货
                #不管正向合约还是反向合约,买卖的手续费都是从结算币种里面扣,不需要做特殊处理,
                #比如BTC正向合约结算币种是USDT,那么就扣USDT,BTC反向合约结算币种是BTC,那么就扣BTC
                gp_df['commission'] = gp_df['fee']
                mult = syminfo['contract_size'] #合约乘数
                if is_inverse: #反向合约
                    #成交额的单位是结算币种,以BTC反向合约为例子,其结算币种为BTC,比如成交100张合约,每张合约价值100美金(合约乘数),
                    #那么就是 100美金*100张合约/成交价=成交额(单位BTC)
                    #turnover = mult * quantity / price
                    pass
                else: #正向合约
                    #正向合约的成交额算法和现货类似,比如成交100张合约,每张合约价值0.01个BTC(合约乘数),
                    #那么就是 0.01BTC*100张合约*成交价=成交额
                    turnover = mult * quantity * price #成交额
            gp_df['BuyVolume'] = (direction + 1) / 2 * quantity
            gp_df['SellVolume'] = (direction - 1) / -2 * quantity
            gp_df['TurnOver'] = turnover
            gp_df['CumVolume'] = quantity.cumsum()
            gp_df['CumTurnOver'] = turnover.cumsum()
            gp_df['CumNetTurnOver'] = (turnover * -direction).cumsum() #累计净成交额
            gp_df['CumNetVolume'] = (quantity * direction).cumsum() #累计净成交量
            if is_spot: #现货
                gp_df['CumProfit'] = (gp_df['CumNetTurnOver'] + gp_df['CumNetVolume'] * price) #累计净成交额+累计净成交量*成交价=累计盈亏(没计算手续费)
                quantity -= (direction + 1) / 2 * gp_df['fee'] #现货买入是从'货'里面扣手续费
                gp_df['position'] = (quantity * direction).cumsum() #去掉手续费后进行计算才是真实仓位
            elif is_future: #期货
                if is_inverse: #反向合约
                    #gp_df['CumProfit'] = (gp_df['CumNetTurnOver'] + gp_df['CumNetVolume'] * mult / price) #比如BTC+BTC
                    #gp_df['position'] = gp_df['CumNetVolume']
                    pass
                else: #正向合约
                    gp_df['CumProfit'] = (gp_df['CumNetTurnOver'] + gp_df['CumNetVolume'] * price * mult) #比如USDT+USDT
                    gp_df['position'] = gp_df['CumNetVolume']
            return gp_df
        #按交易符号分组处理
        gp = df.groupby(by=['platform', 'symbol'])
        self.trades = gp.apply(_apply)

    def process_daily(self):
        """ 每日成交处理
        """
        close = self.closes
        trade = self.trades

        # pro-process
        trade_cols = ['trade_date', 'BuyVolume', 'SellVolume', 'commission', 'CumNetVolume', 'CumNetTurnOver', 'position', 'TurnOver']
        trade = trade.loc[:, trade_cols] #只留下需要的列
        gp = trade.reset_index().groupby(by=['platform', 'symbol', 'trade_date']) #按天分组
        func_last = lambda ser: ser.iat[-1]
        df = gp.agg({'BuyVolume': np.sum, 'SellVolume': np.sum, 'commission': np.sum, 'TurnOver': np.sum,
                     'CumNetVolume': func_last, 'CumNetTurnOver': func_last, 'position': func_last}) #按日统计
        df.index.names = ['platform', 'symbol', 'trade_date']
        #
        df = pd.concat([close, df], axis=1, join='outer') #和每日收盘价连接到一起,如果某一天没有成交,下面会填默认值

        def _apply(gp_df):
            """ 给每日成交统计添加新的列
            """
            platform = gp_df.index.levels[0][0]
            symbol = gp_df.index.levels[1][0]
            syminfo = config.backtest["feature"][platform]["syminfo"][symbol]
            is_spot = False #是否是现货
            is_future = False #是否是期货
            is_inverse = False #如果是期货的话,是否是反向合约
            if syminfo["type"] == "spot": #现货
                is_spot = True
            elif syminfo["type"] == "future": #期货
                is_future = True
                is_inverse = syminfo["is_inverse"]
            #----------------------------------------------
            #如果某一天没有成交,填默认值
            cols_nan_fill = ['close', 'CumNetVolume', 'CumNetTurnOver', 'position']
            gp_df[cols_nan_fill] = gp_df[cols_nan_fill].fillna(method='ffill')
            gp_df[cols_nan_fill] = gp_df[cols_nan_fill].fillna(0)
            #如果某一天没有成交,填默认值
            cols_nan_to_zero = ['BuyVolume', 'SellVolume', 'commission']
            gp_df[cols_nan_to_zero] = gp_df[cols_nan_to_zero].fillna(0)
            #
            mult = 1.0
            #
            close = gp_df['close']
            commission = gp_df['commission']
            cum_net_volume = gp_df['CumNetVolume']
            cum_net_turnOver = gp_df['CumNetTurnOver']
            if is_spot: #现货
                cum_profit = cum_net_turnOver + cum_net_volume * close
            elif is_future: #期货
                mult = syminfo['contract_size'] #合约乘数
                if is_inverse: #反向合约
                    #cum_profit = cum_net_turnOver + mult * cum_net_volume / close
                    pass
                else: #正向合约
                    cum_profit = cum_net_turnOver + mult * cum_net_volume * close
            #
            cum_profit_comm = cum_profit - commission.cumsum()
            gp_df['CumProfit'] = cum_profit #累计盈亏
            gp_df['CumProfitComm'] = cum_profit_comm #计算了手续费后的累计盈亏
            #
            daily_cum_net_turnover_change = cum_net_turnOver.diff(1)
            daily_cum_net_turnover_change.iloc[0] = cum_net_turnOver.iloc[0]
            #
            daily_cum_net_volume_change = cum_net_volume.diff(1)
            daily_cum_net_volume_change.iloc[0] = cum_net_volume.iloc[0]
            if is_spot: #现货
                trading_pnl = (daily_cum_net_turnover_change + close * daily_cum_net_volume_change - commission) #每日交易盈亏(重点:指的是每日)
                holding_pnl = (close.diff(1) * cum_net_volume.shift(1)).fillna(0.0) #每日持仓盈亏(重点:指的是每日)
            elif is_future: #期货
                if is_inverse: #反向合约
                    #trading_pnl = (daily_cum_net_turnover_change + mult * daily_cum_net_volume_change / close - commission)
                    #mult * cum_net_volume.shift(1) / close
                    #holding_pnl = (mult * cum_net_volume.shift(1) / close.diff(1)).fillna(0.0)
                    pass
                else: #正向合约
                    trading_pnl = (daily_cum_net_turnover_change + mult * close * daily_cum_net_volume_change - commission)
                    holding_pnl = (mult * close.diff(1) * cum_net_volume.shift(1)).fillna(0.0)
            #
            total_pnl = trading_pnl + holding_pnl
            gp_df['trading_pnl'] = trading_pnl #每日交易盈亏(重点:指的是每日)
            gp_df['holding_pnl'] = holding_pnl #每日持仓盈亏(重点:指的是每日)
            gp_df['total_pnl'] = total_pnl #每日总盈亏(重点:指的是每日)
            return gp_df
        #按交易符号分组处理
        gp = df.groupby(by=['platform', 'symbol'])
        self.daily = gp.apply(_apply)
        #
        self.daily = self.daily.drop(['CumNetVolume', 'CumNetTurnOver'], axis=1)
        #报告输出的时候需要用到
        gp = self.daily.groupby(by=['platform', 'symbol'])
        for key, value in gp:
            k = key[1] + "@" + key[0]
            self.daily_dic[k] = value

    def fetch_init_balance(self):
        """ 获取策略初始资金
        """
        init_balance = 0
        for x in self.universe:
            platform = x[0]
            symbol = x[1]
            syminfo = config.backtest["feature"][platform]["syminfo"][symbol]
            sc = syminfo["settlement_currency"]
            assets = config.backtest["feature"][platform]["asset"]
            init_balance += assets[sc]
        return init_balance

    def process_returns(self, compound_return=False):
        """ 处理收益率
        """
        cols = ['trading_pnl', 'holding_pnl', 'total_pnl', 'commission', 'CumProfitComm', 'CumProfit', 'TurnOver']
        daily = self.daily.loc[:, cols]
        daily = daily.stack().unstack('platform').unstack('symbol')
        df_pnl = daily.sum(axis=1) #所有交易符号的值加在一起(横截面)
        df_pnl = df_pnl.unstack(level=1)

        init_balance = self.fetch_init_balance() #策略初始资金
        strategy_value = df_pnl['total_pnl'].cumsum()+init_balance #策略资金每日变化列表(策略收益)
        #一个策略收益好不好,需要与一个基准收益进行对比
        benchmark = self.closes.unstack('platform').unstack('symbol').iloc[:, 0] #简单持币收益作为基准收益
        market_values = pd.concat([strategy_value, benchmark], axis=1).fillna(method='ffill') #[策略收益,基准收益]
        market_values.columns = ['strat', 'bench']
        #get strategy & benchmark daily return, cumulative return
        df_returns = market_values.pct_change(periods=1).fillna(0.0)
        df_cum_returns = (df_returns.loc[:, ['strat', 'bench']] + 1.0).cumprod()
        df_returns = df_returns.join(df_cum_returns, rsuffix='_cum')

        if compound_return: #复合收益率
            df_returns.loc[:, 'active_cum'] = df_returns['strat_cum'] - df_returns['bench_cum'] + 1
            df_returns.loc[:, 'active'] = df_returns['active_cum'].pct_change(1).fillna(0.0)
        else:
            df_returns.loc[:, 'active'] = df_returns['strat'] - df_returns['bench'] #策略积极收益率=策略收益率-基准收益率
            df_returns.loc[:, 'active_cum'] = df_returns['active'].cumsum() + 1.0

        #计算最大回撤
        active_cum = df_returns['active_cum'].values
        cum_peak = np.maximum.accumulate(active_cum)
        dd_to_cum_peak = (cum_peak - active_cum) / cum_peak
        max_dd_end = np.argmax(dd_to_cum_peak)  #end of the period
        if max_dd_end > 0:
            max_dd_start = np.argmax(active_cum[:max_dd_end])  #start of period
            max_dd = dd_to_cum_peak[max_dd_end]
        else:
            max_dd_start = 0
            max_dd = 0
        #计算胜率
        win_count = len(df_pnl[df_pnl.total_pnl > 0.0].index)
        lose_count = len(df_pnl[df_pnl.total_pnl < 0.0].index)
        total_count = len(df_pnl.index)
        win_rate = win_count * 1.0 / total_count
        lose_rate = lose_count * 1.0 / total_count
        #
        max_pnl   = df_pnl.loc[:,'total_pnl'].nlargest(1) #最好的一天
        min_pnl   = df_pnl.loc[:,'total_pnl'].nsmallest(1) #最坏的一天
        up5pct    = df_pnl.loc[:,'total_pnl'].quantile(0.95) #95%分位
        low5pct   = df_pnl.loc[:,'total_pnl'].quantile(0.05) #5%分位
        top5_pnl  = df_pnl.loc[:,'total_pnl'].nlargest(5) #最好的五天
        tail5_pnl = df_pnl.loc[:,'total_pnl'].nsmallest(5) #最坏的五天
        #年化收益率,年化波动率,夏普比率
        years = self.period_day / 365.0
        if compound_return:
            self.performance_metrics['Annual Return (%)'] = \
                100 * (np.power(df_returns.loc[:, 'active_cum'].values[-1], 1. / years) - 1)
        else:
            self.performance_metrics['Annual Return (%)'] = \
                100 * (df_returns.loc[:, 'active_cum'].values[-1] - 1.0) / years
        self.performance_metrics['Annual Volatility (%)'] = 100 * (df_returns.loc[:, 'active'].std() * np.sqrt(365))
        self.performance_metrics['Sharpe Ratio'] = (self.performance_metrics['Annual Return (%)']
                                                    / self.performance_metrics['Annual Volatility (%)'])
        #策略总交易次数,总盈亏,日胜率,日输率,总手续费
        self.performance_metrics['Number of Trades']   = len(self.trades.index)
        self.performance_metrics['Total PNL']          = df_pnl.loc[:,'total_pnl'].sum()
        self.performance_metrics['Daily Win Rate(%)']  = win_rate*100
        self.performance_metrics['Daily Lose Rate(%)'] = lose_rate*100
        self.performance_metrics['Commission']         = df_pnl.loc[:,'commission'].sum()
        self.performance_metrics['TurnOver']           = df_pnl.loc[:,'TurnOver'].sum()
        #贝塔值与最大回撤
        self.risk_metrics['Beta'] = np.corrcoef(df_returns.loc[:, 'bench'], df_returns.loc[:, 'strat'])[0, 1]
        self.risk_metrics['Maximum Drawdown (%)']   = max_dd * TO_PCT
        self.risk_metrics['Maximum Drawdown start'] = df_returns.index[max_dd_start]
        self.risk_metrics['Maximum Drawdown end']   = df_returns.index[max_dd_end]
        #回测报告输出
        #业绩指标报告
        self.performance_metrics_report = []
        self.performance_metrics_report.append(('Annual Return (%)',        "{:,.2f}".format(self.performance_metrics['Annual Return (%)']))     )
        self.performance_metrics_report.append(('Annual Volatility (%)',    "{:,.2f}".format(self.performance_metrics['Annual Volatility (%)'])) )
        self.performance_metrics_report.append(('Sharpe Ratio',             "{:,.2f}".format(self.performance_metrics['Sharpe Ratio']))          )
        self.performance_metrics_report.append(('Total PNL',                "{:,.2f}".format(self.performance_metrics['Total PNL']))             )
        self.performance_metrics_report.append(('Commission',               "{:,.2f}".format(self.performance_metrics['Commission']))            )
        self.performance_metrics_report.append(('TurnOver',                 "{:,.2f}".format(self.performance_metrics['TurnOver']))              )
        self.performance_metrics_report.append(('Number of Trades',         self.performance_metrics['Number of Trades'])                        )
        self.performance_metrics_report.append(('Daily Win Rate(%)',        "{:,.2f}".format(self.performance_metrics['Daily Win Rate(%)']))     )
        self.performance_metrics_report.append(('Daily Lose Rate(%)',       "{:,.2f}".format(self.performance_metrics['Daily Lose Rate(%)']))    )
        #日盈亏指标报告
        self.dailypnl_metrics_report = []
        self.dailypnl_metrics_report.append(('Daily PNL Max Time', tools.ts_to_datetime_str(max_pnl.index[0]/1000, fmt='%Y-%m-%d')))
        self.dailypnl_metrics_report.append(('Daily PNL Max',      "{:,.2f}".format(max_pnl.values[0])))
        self.dailypnl_metrics_report.append(('Daily PNL Min Time', tools.ts_to_datetime_str(min_pnl.index[0]/1000, fmt='%Y-%m-%d')))
        self.dailypnl_metrics_report.append(('Daily PNL Min',      "{:,.2f}".format(min_pnl.values[0])))
        self.dailypnl_metrics_report.append(('Daily PNL Up  5%',   "{:,.2f}".format(up5pct)))
        self.dailypnl_metrics_report.append(('Daily PNL Low 5%',   "{:,.2f}".format(low5pct)))

        self.dailypnl_tail5_metrics_report = []
        for k, v in tail5_pnl.iteritems():
            self.dailypnl_tail5_metrics_report.append((tools.ts_to_datetime_str(k/1000, fmt='%Y-%m-%d'), "{:,.2f}".format(v)))

        self.dailypnl_top5_metrics_report = []
        for k, v in top5_pnl.iteritems():
            self.dailypnl_top5_metrics_report.append((tools.ts_to_datetime_str(k/1000, fmt='%Y-%m-%d'), "{:,.2f}".format(v)))
        #风险指标报告
        self.risk_metrics_report = []
        self.risk_metrics_report.append(("Beta",                   "{:,.3f}".format(self.risk_metrics["Beta"]))                                                )
        self.risk_metrics_report.append(("Maximum Drawdown (%)",   "{:,.2f}".format(self.risk_metrics["Maximum Drawdown (%)"]))                                )
        self.risk_metrics_report.append(("Maximum Drawdown start", tools.ts_to_datetime_str(self.risk_metrics["Maximum Drawdown start"]/1000, fmt='%Y-%m-%d')) )
        self.risk_metrics_report.append(("Maximum Drawdown end",   tools.ts_to_datetime_str(self.risk_metrics["Maximum Drawdown end"]/1000, fmt='%Y-%m-%d'))   )

        self.df_pnl = df_pnl
        self.returns = df_returns

    def plot_pnl(self, output_folder):
        """ 生成回测报告所需图片
        """
        for (k, v) in self.daily_dic.items():
            plot_trades(v, symbol=k, output_folder=output_folder)

        old_mpl_rcparams = {k: v for k, v in mpl.rcParams.items()}
        mpl.rcParams.update(MPL_RCPARAMS)

        fig1 = plot_portfolio_bench_pnl(self.returns.loc[:, 'strat_cum'],
                                        self.returns.loc[:, 'bench_cum'],
                                        self.returns.loc[:, 'active_cum'],
                                        self.risk_metrics['Maximum Drawdown start'],
                                        self.risk_metrics['Maximum Drawdown end'])
        fig1.savefig(os.path.join(output_folder, 'pnl_img.png'), facecolor=fig1.get_facecolor(), dpi=fig1.get_dpi())
        plt.close(fig1)

        fig2 = plot_daily_trading_holding_pnl(self.df_pnl['trading_pnl'],
                                              self.df_pnl['holding_pnl'],
                                              self.df_pnl['total_pnl'],
                                              self.df_pnl['total_pnl'].cumsum())
        fig2.savefig(os.path.join(output_folder, 'pnl_img_trading_holding.png'), facecolor=fig2.get_facecolor(), dpi=fig2.get_dpi())
        plt.close(fig2)

        mpl.rcParams.update(old_mpl_rcparams)

    def mts2str(self, daily_dic):
        r = {}
        for (k, v) in daily_dic.items():
            v = v.reset_index()
            v = v.drop(['close'], axis=1)
            v['trade_date'] = [tools.ts_to_datetime_str(mts/1000, fmt='%Y-%m-%d') for mts in v['trade_date'].values]
            v = v.set_index(['platform', 'symbol', 'trade_date']).sort_index(axis=0)
            r[k] = v
        return r

    def gen_report(self, source_dir, template_fn, out_folder='.'):
        """ 生成回测报告
        """
        dic = dict()
        dic['html_title'] = "Strategy Backtest Result"
        dic['props'] = config.backtest
        dic['selected_securities'] = list(self.daily_dic.keys())
        dic['df_daily'] = self.mts2str(self.daily_dic)
        dic['performance_metrics_report'] = self.performance_metrics_report
        dic['risk_metrics_report'] = self.risk_metrics_report
        dic['dailypnl_metrics_report'] = self.dailypnl_metrics_report
        dic['dailypnl_top5_metrics_report'] = self.dailypnl_top5_metrics_report
        dic['dailypnl_tail5_metrics_report'] = self.dailypnl_tail5_metrics_report

        self.report_dic.update(dic)

        r = Report(self.report_dic, source_dir=source_dir, template_fn=template_fn, out_folder=out_folder)
        r.generate_html()
        r.output_html('report.html')

    def do_analyze(self, result_dir):
        """ 开始分析,并且生成回测报告
        """
        tools.create_dir(os.path.join(os.path.abspath(result_dir), 'dummy.dummy')) #创建保存回测结果的目录
        self.process_trades()
        self.process_daily()
        self.process_returns()
        self.plot_pnl(result_dir)
        self.gen_report(source_dir=STATIC_FOLDER, template_fn='report_template.html', out_folder=result_dir)

def plot_daily_trading_holding_pnl(trading, holding, total, total_cum):
    """
    """
    idx0 = total.index
    n = len(idx0)
    idx = np.arange(n)

    fig, (ax0, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 13.5), sharex=True)
    ax1 = ax0.twinx()

    bar_width = 0.4
    profit_color, lose_color = '#D63434', '#2DB635'
    curve_color = '#174F67'
    y_label = 'Profit / Loss ($)'
    color_arr_raw = np.array([profit_color] * n)

    color_arr = color_arr_raw.copy()
    color_arr[total < 0] = lose_color
    ax0.bar(idx, total, width=bar_width, color=color_arr)
    ax0.set(title='Daily PnL', ylabel=y_label, xlim=[-2, n+2],)
    ax0.xaxis.set_major_formatter(MyFormatter(idx0, '%y-%m-%d'))

    ax1.plot(idx, total_cum, lw=1.5, color=curve_color)
    ax1.set(ylabel='Cum. ' + y_label)
    ax1.yaxis.label.set_color(curve_color)

    color_arr = color_arr_raw.copy()
    color_arr[trading < 0] = lose_color
    ax2.bar(idx-bar_width/2, trading, width=bar_width, color=color_arr)
    ax2.set(title='Daily Trading PnL', ylabel=y_label)

    color_arr = color_arr_raw.copy()
    color_arr[holding < 0] = lose_color
    ax3.bar(idx+bar_width/2, holding, width=bar_width, color=color_arr)
    ax3.set(title='Daily Holding PnL', ylabel=y_label, xticks=idx[: : n//10 + 1])
    return fig

def plot_portfolio_bench_pnl(portfolio_cum_ret, benchmark_cum_ret, excess_cum_ret, max_dd_start, max_dd_end):
    """
    """
    n_subplots = 3
    fig, (ax1, ax2, ax3) = plt.subplots(n_subplots, 1, figsize=(16, 4.5 * n_subplots), sharex=True)
    idx_dt = portfolio_cum_ret.index
    idx = np.arange(len(idx_dt))

    y_label_ret = "Cumulative Return (%)"

    ax1.plot(idx, (benchmark_cum_ret-1) * TO_PCT, label='Benchmark', color='#174F67')
    ax1.plot(idx, (portfolio_cum_ret-1) * TO_PCT, label='Strategy', color='#198DD6')
    ax1.legend(loc='upper left')
    ax1.set(title="Absolute Return of Portfolio and Benchmark", 
            #xlabel="Date", 
            ylabel=y_label_ret)
    ax1.grid(axis='y')

    ax2.plot(idx, (excess_cum_ret-1) * TO_PCT, label='Extra Return', color='#C37051')
    ax2.axvspan(idx_dt.get_loc(max_dd_start), idx_dt.get_loc(max_dd_end), color='lightgreen', alpha=0.5, label='Maximum Drawdown')
    ax2.legend(loc='upper left')
    ax2.set(title="Excess Return Compared to Benchmark", ylabel=y_label_ret
            #xlabel="Date", 
            )
    ax2.grid(axis='y')
    ax2.xaxis.set_major_formatter(MyFormatter(idx_dt, '%y-%m-%d'))  # 17-09-31

    ax3.plot(idx, (portfolio_cum_ret ) / (benchmark_cum_ret ), label='Ratio of NAV', color='#C37051')
    ax3.legend(loc='upper left')
    ax3.set(title="NaV of Portfolio / NaV of Benchmark", ylabel=y_label_ret
            #xlabel="Date",
           )
    ax3.grid(axis='y')
    ax3.xaxis.set_major_formatter(MyFormatter(idx_dt, '%y-%m-%d'))  # 17-09-31

    fig.tight_layout()  
    return fig

def plot_trades(df, symbol="", output_folder='.', marker_size_adjust_ratio=0.1):
    """
    """
    old_mpl_rcparams = {k: v for k, v in mpl.rcParams.items()}
    mpl.rcParams.update(MPL_RCPARAMS)

    idx0 = df.index
    idx = range(len(idx0))

    price = df.loc[:, 'close']
    bv, sv = df.loc[:, 'BuyVolume'].values, df.loc[:, 'SellVolume'].values
    profit = df.loc[:, 'CumProfit'].values
    
    bv_m = np.max(bv)
    sv_m = np.max(sv)
    if bv_m > 0:
        bv = bv / bv_m * 100
    if sv_m > 0:
        sv = sv / sv_m * 100
    
    fig = plt.figure(figsize=(14, 10))
    ax1 = plt.subplot2grid((4, 1), (0, 0), rowspan=3)
    ax3 = plt.subplot2grid((4, 1), (3, 0), rowspan=1, sharex=ax1)
    
    ax2 = ax1.twinx()
    
    ax1.plot(idx, price, label='Price', linestyle='-', lw=1, marker='', color='yellow')
    ax1.scatter(idx, price, label='buy', marker='o', s=bv, color='indianred')
    ax1.scatter(idx, price, label='sell', marker='o', s=sv, color='forestgreen')
    ax1.legend(loc='upper left')
    ax1.set(title="Price, Trades and PnL for {:s}".format(symbol), ylabel="Price ($)")
    ax1.xaxis.set_major_formatter(MyFormatter(idx0.levels[2], '%Y-%m-%d'))
    
    ax2.plot(idx, profit, label='PnL', color='k', lw=1, ls='--', alpha=.4)
    ax2.legend(loc='upper right')
    ax2.set(ylabel="Profit / Loss ($)")
    
    # ax1.xaxis.set_major_formatter(MyFormatter(df.index))#, '%H:%M'))
    
    ax3.plot(idx, df.loc[:, 'position'], marker='D', markersize=3, lw=2)
    ax3.axhline(0, color='k', lw=1, ls='--', alpha=0.8)
    ax3.set(title="Position of {:s}".format(symbol))
    
    fig.tight_layout()
    fig.savefig(output_folder + '/' + "{}.png".format(symbol.replace('/','').lower()), facecolor=fig.get_facecolor(), dpi=fig.get_dpi())
    plt.close(fig)

    mpl.rcParams.update(old_mpl_rcparams)