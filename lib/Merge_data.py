import pandas as pd
import xlrd
import csv
import os

import sys as system

data_root_path = '../data/01-raw/06-Ulsan/'
basic_weather_data_file_path = data_root_path + 'weather.csv'
basic_PM_data_file_path = data_root_path + 'particulate/'

new_data_file_path = '../data/02-new/06-Ulsan/'
city = 'Ulsan'
new_weather_file_path = new_data_file_path + city + '-weather.csv'
new_particulate_matter_file_path = new_data_file_path + city + '-'

count_observatory = 16
start_year = 2016
last_year = 2018

list_new_file_path = []


def weather_data_save():
    ''' 날씨 데이터 변경 '''
    with open(basic_weather_data_file_path, 'r') as raw_file:
        file = csv.reader(raw_file)

        csv_new = open(new_weather_file_path, 'w', encoding='utf-8', newline='')
        csv_writer = csv.writer(csv_new)
        csv_writer.writerow(['date', 'temperatures', 'wind_velocity', 'wind_direction', 'relative_humidity'])

        line_index = 0

        for row in file:
            if not line_index == 0:
                date = row[1]
                temperature = row[2]
                wind_velocity = row[3]
                wind_direction = row[4]
                relative_humidity = row[5]

                csv_writer.writerow([date, temperature, wind_velocity, wind_direction, relative_humidity])

            line_index += 1

    list_new_file_path.append(new_weather_file_path)
    print("Raw weather Data: old csv -> csv     >>>>> Done")


def particulate_matter_data(year):
    ''' 해당 년도 미세먼지 데이터 합치기 '''
    separate = '/'
    file_extension = '.xls'
    new_file_extension = '.csv'

    new_file_path = new_particulate_matter_file_path + str(year) + new_file_extension

    list_new_file_path.append(new_file_path)

    csv_new = open(new_file_path, 'w', encoding='utf-8', newline='')
    csv_writer = csv.writer(csv_new)
    csv_writer.writerow(['date', 'PM-10', 'PM-2.5', 'O3', 'NO2', 'CO', 'SO2'])

    for i in range(1, 13):
        for j in range(1, count_observatory):
            raw_file_path = basic_PM_data_file_path + str(year) + separate + str(i) + '-' + str(j) + file_extension
            csv_writer = particulate_matter_file_open_and_write(raw_file_path, csv_writer)

    print(str(year) + ": particulate matter Data: xlsx -> csv >>>> Done")


def particulate_matter_file_open_and_write(filename, csv_writer):
    ''' 하나의 미세먼지 파일 값 쓰기 '''
    xlsx = xlrd.open_workbook(filename)
    load_dust = xlsx.sheet_by_index(0)

    for row in range(load_dust.nrows):

        # print(str(load_dust.row_values(row)))
        if row > 1:
            date = load_dust.row_values(row)[0]
            PM10 = load_dust.row_values(row)[1]
            PM25 = load_dust.row_values(row)[2]
            ozon = load_dust.row_values(row)[3]
            C2N = load_dust.row_values(row)[4]
            CO = load_dust.row_values(row)[5]
            gas = load_dust.row_values(row)[6]

            csv_writer.writerow([date, PM10, PM25, ozon, C2N, CO, gas])

    return csv_writer


def file_open(from_year, to_year):
    file_extension = '.csv'

    pd_weather = pd.read_csv(new_weather_file_path)
    list_pd_basic_pm = []

    for i in range(from_year, to_year + 1):
        pd_pm = pd.read_csv(new_particulate_matter_file_path + str(i) + file_extension)
        pd_pm.sort_values(['date'], ascending=[True])
        list_pd_basic_pm.append(pd_pm)

    return pd_weather, list_pd_basic_pm


def remove_file():
    ''' 파일 삭제 '''
    for i in range(len(list_new_file_path)):
        os.remove(list_new_file_path[i])

    print("파일 삭제 완료")
    # os.remove(file_path)


def convert_data_to_csv():
    weather_data_save()
    for i in range(start_year, last_year + 1):
        particulate_matter_data(i)


def merge_weather_and_pm_data():
    convert_data_to_csv()

    pd_weather, list_pd_basic_pm = file_open(start_year, last_year)

    list_merge_data = []

    for i in range(len(list_pd_basic_pm)):
        data = pd.merge(list_pd_basic_pm[i], pd_weather, on="date")
        data['date'] = pd.to_datetime(data['date'])
        data = data.sort_values(['date'], ascending=[True])
        list_merge_data.append(data)
        print(str(start_year + i) + ": merge weather data and PM data >>>> Complete")

    return list_merge_data


def divide_by_label():
    list_merge_data = merge_weather_and_pm_data()
    file_extension = '.csv'

    for i in range(len(list_merge_data)):
        data = list_merge_data[i]
        data = data.drop(['date'], axis=1)
        pm10_data = data.drop(['PM-2.5'], axis=1)
        pm25_data = data.drop(['PM-10'], axis=1)

        pm10_data = remove_null_value(pm10_data)
        pm25_data = remove_null_value(pm25_data)

        pm10_data.to_csv(new_particulate_matter_file_path + str(start_year+i)+'_pm10'+file_extension, index=False)
        pm25_data.to_csv(new_particulate_matter_file_path + str(start_year+i)+'_pm2.5'+file_extension, index=False)

    remove_file()
    print('\n')


def remove_null_value(data):
    data = data.dropna()

    return data

ogressing
if __name__ == '__main__':
    divide_by_label()
