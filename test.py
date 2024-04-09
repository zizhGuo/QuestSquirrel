import os
import subprocess
import sys
import yaml
import logging

CONFIG = """
tables: [
    'dws_envir_spirit.xlsx'
    ,'dws_envir_spirit_breakthru.xlsx'
    ,'dws_envir_pet.xlsx'
    ,'dws_envir_pet_breakthru.xlsx'
]
"""




# class Logger:
#     def __init__(self):
#         pass

#     def print(self, a, b):
#         return a*b

# def func(a, b, **kwargs):
#     print(kwargs)
#     for key, value in kwargs.items():
#         if key == 'logger':
#             print(value.print(a, b))
#             return
#     print(a+b)

# def func2(a, b, **kwargs):
#     if kwargs.get('logger'):
#         print(a * b)
#         return
#     logger = kwargs.get('logger')
#     if logger:
#         print(a * b)
#         return
#     print(a + b)

# class LoggerSingleton(object):
#     def __init__(self) -> None:
#         _instance = None
    
#     def __new__(cls):
#         if cls._instance is None:
#             print('Creating the object')
#             cls._instance = super(LoggerSingleton, cls).__new__(cls)
#             # Put any initialization here.
#         return cls._instance1




class Test:
    def __init__(self) -> None:
        pass

    def func_logging(self):
        logging.debug("This is a debug log.")
        logging.info("This is a info log.")
        logging.warning("This is a warning log.")
        logging.error("This is a error log.")
        logging.critical("This is a critical log.")

def main():
    import pandas as pd
    import numpy as np
    # create a dataframe 
    df = pd.DataFrame(np.random.randn(4, 3), columns=['a', 'b', 'c'])
    print(df)
    columns = ['c', 'b']
    df = df[columns]
    print(df)

if __name__ == "__main__":
    # logger = LoggerSingleton()

    logging.debug("This is a debug log.")
    logging.info("This is a info log.")
    logging.warning("This is a warning log.")
    logging.error("This is a error log.")
    logging.critical("This is a critical log.")
    tester = Test()
    tester.func_logging()

    main()

