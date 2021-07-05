import requests
import numpy as np
import pandas as pd
import json
import sqlalchemy
from sqlalchemy import create_engine
from datetime import date
from datetime import timedelta
from datetime import datetime
import os

# token
token = "SJRNtbHkWTiiNIkoVWBmPtHqWxYIjZJj"
header = {"token": token}

# url
base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/'


def city_lookup_table():
    cities = []

    # url to get all us cities
    url = base_url + "locations?sortfield=id&sortorder=asc&locationcategoryid=CITY&limit=1000&offset=1071"

    response = requests.get(url, headers=header)

    json_data = response.json()

    # get all us cities
    index = 0
    for key in json_data['results']:
        if key['id'].startswith('CITY:US'):
            cities.append({'name': key['name']})
            cities[index].update({'id': key['id']})
            index = index + 1

    # remove US from each city name
    for key in cities:
        key['name'] = key['name'].replace(' US', '')

    return cities


def weather_stations(cities, city_input):
    stations = []
    # look for city, get station then get temperature
    for city in cities:
        if city_input == city['name']:

            # query url
            endpoint = '&datacategoryid=TEMP&datasetid=GHCND&sortfield=datacoverage&sortorder=desc'
            datatypes = f'&datatypeid=TMAX'
            url = base_url + f'stations?locationid={city["id"]}' + endpoint + datatypes

            # get stations for city
            response = requests.get(url, headers=header)
            data = response.json()

            # get station with most coverage and period of record
            for i in data['results']:
                d1 = date.today()
                d2 = date(1970, 1, 1)

                maxdate_obj = datetime.strptime(i['maxdate'], '%Y-%m-%d')
                mindate_obj = datetime.strptime(i['mindate'], '%Y-%m-%d')
                maxdate_obj = maxdate_obj.date()
                mindate_obj = mindate_obj.date()

                # maxdate within one week from today, mindate lessthan or equal to Jan 1st, 1970
                if (d1 - maxdate_obj).days <= 7 and mindate_obj <= d2 and 1 >= i['datacoverage'] > .8:
                    stations.append(i)

            # sort list by datacoverage in descending order
            stations = sorted(stations, key=lambda k: k['datacoverage'], reverse=True)
    return stations


def city_weather():
    # prompt user to enter city
    print('Enter a city: (ex. Charlotte, NC)')
    city_input = input()
    cities = city_lookup_table()
    stations = weather_stations(cities, city_input)

    # make request for temperature
    datatype = 'TMAX'
    startdate = date.today() - timedelta(days=14)
    enddate = date.today() - timedelta(days=7)

    # iterate through each station, see if it has requested data
    # if it does then make request, if not proceed to check next station
    for station in stations:
        station_id = station['id']
        endpoint = f'data?datasetid=GHCND&datatypeid={datatype}&stationid={station_id}&startdate={startdate}&enddate={enddate}'
        url = base_url + endpoint + '&limit=7&units=standard'
        response = requests.get(url, headers=header)
        data = response.json()

        if data:
            # parse temp data (values and dates)
            temps = []
            index = 0
            for item in data["results"]:
                temps.append({'date': datetime.strptime(item['date'].replace('T00:00:00', ''), '%Y-%m-%d').date()})
                temps[index].update({'value': item['value']})
                index += 1
            return temps
        else:
            continue


city_temps = city_weather()
df = pd.DataFrame(city_temps)

# create engine object
engine = create_engine('mysql://root:codio@localhost/weather')
df.to_sql('max_temps', con=engine, if_exists='replace', index=False)

# save database
os.system('mysqldump -u root -pcodio weather > weatherapi.sql')