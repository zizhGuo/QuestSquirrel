# 重试装饰器
#
# 作者: 郭子谆
# 日期: 2024.06.12
import functools
import time

def retry(max_retries=3, retry_delay=5, type='default connection',exception_to_check=Exception):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    print(f'{type} retrying...  now the \'{attempts+1}\' attempts')
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    print(f"Error: {e}, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    attempts += 1
            print("Max retries reached, exiting.")
            # sys.exit(1)
        return wrapper
    return decorator_retry
