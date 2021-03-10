import time
import os
from datetime import datetime
import dateutil.parser as dparser


def timestamp_datetime(secs):
    dt = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(secs))
    return dt


def datetime_timestamp(dt):
    s = time.mktime(time.strptime(dt, '%Y-%m-%dT%H:%M:%SZ'))
    return int(s)


def add_TZ(dt):
    return dparser.parse(dt).strftime('%Y-%m-%dT%H:%M:%SZ')


def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
