import functools
import time
import sys

def retry(max_retries=3, retry_delay=5, exception_to_check=Exception):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    print('retrying...  now the \'{}\' attempts'.format(attempts+1))
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    print(f"Error: {e}, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    attempts += 1
                    if attempts >= max_retries:
                        print("Max retries reached, re-raising the exception.")
                        raise e
        return wrapper
    return decorator_retry

def retry_inner_except(max_retries=3, retry_delay=5, exception_to_check=Exception):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    print(f'retrying... now the {attempts + 1} attempts')
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    print('inner except, retrying...')
                    time.sleep(retry_delay)
                    attempts += 1
                    if attempts >= max_retries:
                        print("Max retries reached, re-raising the exception.")
                        raise e
        return wrapper
    return decorator_retry

@retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
def test_outer_except():
    raise Exception('test')

if __name__ == '__main__':
    try:
        test_outer_except()
    except Exception as e:
        print(f'Caught in main: {e}')