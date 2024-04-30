
import os
import functools
import pandas as pd
from numpy import nan


import sys
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'transform')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'transform')))


def drop_columns(func):
    """装饰器：删除指定列
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, columns, *args, **kwargs):
        print(df)
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
        当前功能: 将nan值替换为modify_dict中对应col的值
        先于func执行
    """
    @functools.wraps(func)
    def wrapper(self, df, modify_dict, *args, **kwargs):
        for col, val in modify_dict.items():
            print(f"!!!!!!!!!!!!!!!!!!!!!!!! {col} {val}")
            
            df[col] = df[col].apply(lambda x: nan if x == val else x)
        print(df)
        func(self, df, modify_dict, *args, **kwargs)
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