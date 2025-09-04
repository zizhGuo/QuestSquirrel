
import os
import functools
import pandas as pd
from numpy import nan


import sys
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'transform')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'transform')))


def columns_switch(func):
    """装饰器: 列的顺序调整
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, columnsleft:list, columnsright:list, *args, **kwargs):
        remaining_columns = [col for col in df.columns if col not in columnsleft + columnsright ]
        new_columns = remaining_columns + columnsright + columnsleft
        func(self, *args, **kwargs)
        df = df[new_columns]
        return df
    return wrapper

def modify_column_type(func):
    """装饰器: 修改列的数据类型
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, modify_dict, *args, **kwargs):
        for col, val in modify_dict.items():
            df[col] = df[col].astype(val)
        func(self, *args, **kwargs)
        return df
    return wrapper

def sum_columns(func):
    """装饰器: 计算指定列的和，新创建一列
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, columns, sum_col:str, *args, **kwargs):
        df[sum_col] = df[columns].sum(axis=1)
        func(self, *args, **kwargs)
        return df
    return wrapper

def ratio_columns(func):
    """装饰器: 计算指定列（多个）和指定列的比值，并创建指定比值列
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, columns, sum_col, ratio_columns, *args, **kwargs):
        for col, ratio_col in zip(columns, ratio_columns):
            df[ratio_col] = df[col] / df[sum_col]
            df[ratio_col] = df[ratio_col].apply(lambda x: f"{x:.2%}")
        func(self, *args, **kwargs)
        return df
    return wrapper

def drop_columns(func):
    """装饰器：删除指定列
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, columns, *args, **kwargs):
        df = df.drop(columns, axis=1)
        func(self, *args, **kwargs)
        # 改动
        # df = func(self, df, *args)
        return df
    return wrapper

def select_columns(func):
    """装饰器: 选择指定列
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, columns_dict, *args, **kwargs):
        for col, val in columns_dict.items():
            df = df[df[col] == val].reset_index(drop=True)
        func(self, df, columns_dict, *args, **kwargs)
        # df = func(self, df, columns_dict, *args)
        return df
    return wrapper

def modify_cell_matched(func):
    """装饰器: 修改指定单元格
        当前功能: 将modify_dict中对应col的值替换为nan值
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, modify_dict, *args, **kwargs):
        for col, val in modify_dict.items():
            df[col] = df[col].apply(lambda x: nan if x == val else x)
        func(self, df, modify_dict, *args, **kwargs)
        return df
    return wrapper

def modify_cell_matched_v2(func):
    """装饰器: 修改指定单元格
        当前功能: 将modify_dict中col的值替换为nan值
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df,  *args, **kwargs):
        if 'all' in kwargs:
            _val = kwargs['all']
            df = df.replace(_val, nan)
        func(self, df, *args, **kwargs)
        return df
    return wrapper  

def modify_cell_matched_v3(func):
    """装饰器: 修改指定单元格
        当前功能: 将list 中的所有cell替换为给定值
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, target_col, replaced_val = '\\', *args, **kwargs):
        if not isinstance(target_col, str):
            raise Exception('target_col is not string')
        c_index = df.columns.get_loc(target_col)
        for i in range(0, len(df)):
            for j in range(c_index, c_index + i + 1):
                if j < len(df.columns):
                    df.iat[i, j] = replaced_val
        # if 'all' in kwargs:
        #     _val = kwargs['all']
        #     df = df.replace(_val, nan)
        # func(self, df, *args, **kwargs)
        return df
    return wrapper 

# def modify_dataframe(func):
#     """装饰器: 修改指定数据框
#         先于func执行
#     """
#     @functools.wraps(func)
#     def wrapper(self, df, modify_dict, *args):
#         for col, val in modify_dict.items():
#             df[col] = val
#         func(self, df, modify_dict, *args)
#         return df
#     return wrapper


# def filter_cells(func):
#     """装饰器: 过滤指定单元格
#         先于func执行
#     """
#     @functools.wraps(func)
#     def wrapper(self, df, filter_dict, *args):
#         for col, val in filter_dict.items():
#             df = df[df[col].str.contains(val, na=False)].reset_index(drop=True)
#         func(self, df, filter_dict, *args)
#         return df
#     return wrapper