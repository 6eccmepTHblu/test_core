import time
import functools


def execution_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        total_time = round(time.time() - start_time, 4)
        print(f"Время выполнения `{func.__name__}` - {total_time} сек.")
        return result

    return wrapper
