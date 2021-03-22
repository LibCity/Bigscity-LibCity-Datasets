import time
import os


def timestamp_datetime(secs):
    dt = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(secs))
    return dt


def datetime_timestamp(dt):
    s = time.mktime(time.strptime(dt, '%Y-%m-%dT%H:%M:%SZ'))
    return int(s)


def add_tz(dt):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(dt, '%Y-%m-%d %H:%M:%S'))


def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
