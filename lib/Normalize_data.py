import lib.Merge_data as merge
import pandas as pd
import os

import sys as system

data_root_path = '../data/02-new/06-Ulsan/'
city = 'Ulsan'
weather_file_path = data_root_path + city + '-weather.csv'
particulate_matter_file_path = data_root_path + city + '-'

start_year = 2016
last_year = 2018


def file_open(file_path):
    pd_data = pd.read_csv(file_path)

    return pd_data


def normalize_function(max_data, min_data, value):
    x = (value - min_data) / (max_data - min_data)
    return x


def normalize_wind_direction(data):
    try:
        value = float(data)
    except ValueError:
        value = 200

    if 22.5 < value <= 67.5:  # 북동
        return 1

    elif value <= 112.5:  # 동
        return 2

    elif value <= 157.5:  # 남동
        return 3

    elif value <= 202.5:  # 남
        return 4

    elif value <= 247.5:  # 남서
        return 5

    elif value <= 292.5:  # 서
        return 6

    elif value <= 337.5:  # 북서
        return 7

    else:  # 북
        return 0


def normalize_file_data(year, pm_num):
    file_path = particulate_matter_file_path + str(year) + '_pm' + str(pm_num) + '.csv'

    pd_data = file_open(file_path)
    data_max = pd_data.max(axis=0)
    data_min = pd_data.min(axis=0)
    data_column = pd_data.columns
    data_count_row = len(pd_data)

    for i in range(1, len(data_column)):
        for j in range(data_count_row):
            x = pd_data[data_column[i]][j]

            if data_column[i] == 'wind_direction':
                data = normalize_wind_direction(x)
            else:
                data = normalize_function(data_max[i], data_min[i], x)

            pd_data[data_column[i]][j] = data

    pd_data.to_csv(particulate_matter_file_path + str(year) + '-pm' + str(pm_num) + '.csv', index=False)
    os.remove(file_path)
    print('Normalize complete: ' + str(year) + ' -> pm' + str(pm_num))


def normalize():
    merge.divide_by_label()

    for i in range(start_year, last_year + 1):
        normalize_file_data(i, 10)
        normalize_file_data(i, 2.5)

    print('\nData processing Complete')


if __name__ == '__main__':
    normalize()
