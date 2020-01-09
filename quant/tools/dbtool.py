# -*- coding:utf-8 -*-

"""
DBTool module.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

class DBTool(object):

    def __init__(self):
        """ Initialize. """

    """
    get_df_from_table: 根据table名字,获取dataframe
    query_df_from_table: 根据table名字，过滤条件(起始时间，某个字段取值范围等)，获取数据
    dump_csv_from_table: 根据table名字,dump csv文件
    query_csv_from_table: 根据table名字，过滤条件(起始时间，某个字段取值范围等)，dump csv文件
    get_df_from_csv: 根据路径，获取dataframe
    query_df_from_csv: 根据路径，过滤条件(起始时间，某个字段取值范围等)，获取dataframe
    write_csv_from_df: 给定数据，目标路径，写入csv
    to_daily_csv_from_table: 生成日数据
    left_join: 给定左边dataframe，给定右边dataframe，把它们合并（按照msecond，要求针对左边的表里的每一行，在右边表里找到对应左边时间点之前的最近一条数据）
    """

    def get_df_from_table(self, table_name, fields=None):
        """ 根据table名字,获取dataframe
        """
        pass

    def query_df_from_table(self, table_name, condition_str_list, fields=None):
        """ 根据table名字，过滤条件(起始时间，某个字段取值范围等)，获取数据
        """
        pass

    def dump_csv_from_table(self, table_name, fields=None):
        """ 根据table名字,dump csv文件
        """
        pass

    def query_csv_from_table(self, table_name, condition_str_list, fields=None):
        """ 根据table名字，过滤条件(起始时间，某个字段取值范围等)，dump csv文件
        """
        pass

    def get_df_from_csv(self, file_path, fields=None):
        """ 根据路径，获取dataframe
        """
        pass

    def query_df_from_csv(self, file_path, condition_str_list, fields=None):
        """ 根据路径，过滤条件(起始时间，某个字段取值范围等)，获取dataframe
        """
        pass

    def write_csv_from_df(self, df, file_path):
        """ 给定数据，目标路径，写入csv
        """
        pass

    def to_daily_csv_from_table(self, table_name, file_path):
        """ 根据table名字，把数据转化成年月日文件夹存储的csv数据
        """
        pass

    def left_join(self, df_left, df_right, tolerance_millisecond):
        """ 给定左边dataframe，给定右边dataframe，把它们合并（按照millisecond，要求针对左边的表里的每一行，在右边表里找到对应左边时间点之前的最近一条数据
        但是必须在容忍微秒数之内，否则匹配None）
        """
        pass




