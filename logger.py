from functools import wraps
import logging
import time

parsing_logger = logging.getLogger('PARSER')
parsing_logger.setLevel(logging.INFO)

time_logger = logging.getLogger('TIMER')
time_logger.setLevel(logging.INFO)


def timer_decorator(log_msg):
    def timing(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            s_time = time.time()
            result = f(*args, **kwargs)
            f_time = time.time()
            logging.getLogger('TIMER').info('It took {:.2f}s to {}.'.format(f_time - s_time, log_msg))
            return result
        return wrapper
    return timing
