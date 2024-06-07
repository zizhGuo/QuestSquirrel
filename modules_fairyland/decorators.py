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

# def send_email(func, sender_config, error_msg):
#     @functools.wraps(func)
#     def wrapper(self, *args, **kwargs):
#         print('error_msg: ', error_msg)

#         self.message = config[module.email_module][sub_email]['message']
#         self.subject = config[module.email_module][sub_email]['subject']
#         self.header_from = config[module.email_module][sub_email]['header_from']
#         self.sender_email = config[module.email_module][sub_email]['sender_email']
#         self.header_to = config[module.email_module][sub_email]['header_to']
#         self.recipient_show = config[module.email_module][sub_email]['recipient_show']
#         self.cc_show = config[module.email_module][sub_email]['cc_show']
#         self.user = config[module.email_module][sub_email]['user']
#         self.password = config[module.email_module][sub_email]['password']
#         self.to_addrs = config[module.email_module][sub_email]['to_addrs']



#         func(self, *args, **kwargs)
#     return wrapper