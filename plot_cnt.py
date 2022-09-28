import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
import matplotlib.dates as mdates
from tqdm import tqdm
import datetime

# datetime.datetime.utcfromtimestamp(tim)

def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def get_detail(x):
    # x = eval(x)[0]
    x = datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ')
    return x.minute, x.hour, x.weekday()


def draw_time_of_day(data, data_col='porto', timesolts=10, ylabel='count of trajectories', onlyday=None):
    min2cnt = np.zeros(24 * 60 // timesolts + 1)
    for i in tqdm(range(data.shape[0])):
        m, h, d = get_detail(data.iloc[i])
        mins = (h * 60 + m) // timesolts
        if onlyday is not None:
            if d != onlyday:
                continue
        min2cnt[mins] += 1
    min2cnt[-1] = min2cnt[0]

    if onlyday is None:
        file_name = './{}_time_of_day.png'.format(data_col)
        title_name = data_col
    else:
        file_name = './{}_{}_time_of_day.png'.format(data_col, onlyday)
        title_name = data_col + '_only Weeks=' + str(onlyday)

    fig, ax = plt.subplots(figsize=(12, 9))
    hours = mdates.HourLocator(interval=48)
    ax.xaxis.set_major_locator(hours)

    date_rng = pd.date_range('2020-12-01', '2020-12-02', freq='{}S'.format(timesolts * 60))
    dates = date_rng.strftime('%H:%M').tolist()
    dates[-1] = '24:00'
    ax.plot(dates, min2cnt)
    ax.set_title(title_name)
    ax.set_xlabel('time of day')
    ax.set_ylabel(ylabel)
    ax.set_xticks(range(0, 24 * 60 // timesolts + 1, 60 // timesolts))
    ax.grid(True)
    fig.tight_layout()
    fig.autofmt_xdate()
    plt.savefig(file_name)
    plt.show()
    plt.close()


def draw_day_of_week(data, data_col='Traj', timesolts=10, ylabel='cnt', node_id=None):
    entity_id, traj_id = -1, -1
    min2cnt = np.zeros(7 * 24 * 60 // timesolts + 1)  # 7*24小时
    for i in tqdm(range(data.shape[0])):
        eid = data.iloc[i]['entity_id']
        tid = data.iloc[i]['traj_id']
        if eid != entity_id or tid != traj_id:
            ts = data.iloc[i]['time']
            m, h, d = get_detail(data.iloc[i])
            mins = ((h * 60 + m) // timesolts) + (d * 24 * 60 // timesolts)
            min2cnt[mins] += 1
        else:
            continue
    min2cnt[-1] = min2cnt[0]
    np.save('{}.npy'.format(data_col), min2cnt)

    fig, ax = plt.subplots(figsize=(12, 9))
    hours = mdates.WeekdayLocator(interval=1)
    ax.xaxis.set_major_locator(hours)

    save_path = './{}_day_of_week.png'.format(data_col)
    title = data_col
    if node_id is not None:
        save_path = './{}_day_of_week_{}.png'.format(data_col, node_id)
        title = data_col + ' Node {}'.format(node_id)

    ax.plot(np.arange(7 * 24 * 60 // timesolts + 1), min2cnt)
    ax.set_title(title)
    ax.set_xlabel('day of week')
    ax.set_ylabel(ylabel)
    ax.set_xticks(range(0, 7 * 24 * 60 // timesolts + 1, 24 * 60 // timesolts))
    ax.grid(True)
    plt.gca().set_xticklabels(['', 'Mon', 'Tues', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    fig.autofmt_xdate()
    plt.savefig(save_path)
    plt.show()
    plt.close()


data_col = 'chengdu'
if data_col == 'porto':
    # data = pd.read_csv('raw_data/{0}/{0}/{0}_merge.csv'.format(data_col), sep=';')
    data = pd.read_csv('raw_data/train.csv')
elif data_col == 'selected1_all':
    data = pd.read_csv('raw_data/201511/{0}/{0}_merge.csv'.format(data_col), sep=';')
elif data_col == 'chengdu':
    data = pd.read_csv('output/cd_traj_1101_1110/cd_traj_1101_1110.dyna')

# draw_time_of_day(data['TIMESTAMP'], data_col, onlyday=5)
# draw_time_of_day(data['TIMESTAMP'], data_col, onlyday=6)
draw_day_of_week(data, data_col)
