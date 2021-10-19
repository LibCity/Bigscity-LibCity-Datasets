import time
import os
import datetime


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


def int_to_isoformat(_time):
    days = _time // 86400
    seconds = _time - days * 86400
    hours = seconds // 3600
    minutes = (seconds - 3600 * hours) // 60
    seconds = seconds - 3600 * hours - minutes * 60

    delta_time = datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    cur_time = datetime.datetime(1970, 1, 1) + delta_time
    cur_time = cur_time.isoformat()
    return cur_time
