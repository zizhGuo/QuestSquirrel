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
            print("Max retries reached, exiting.")
            # sys.exit(1)
        return wrapper
    return decorator_retry