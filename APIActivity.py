import requests
import numpy as np
import pandas as pd
import json
import sqlalchemy
from sqlalchemy import create_engine
from datetime import date
from datetime import timedelta
from datetime import datetime

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


def city_weather(datatype, city, startdate, enddate):
    cities = city_lookup_table()
    stations = weather_stations(cities, city)

    # make request for temperature
    #startdate = date.today() - timedelta(days=7)
    #enddate = date.today()

    # iterate through each station, see if it has requested data
    # if it does then make request, if not proceed to check next station
    for station in stations:

        # station from which to query data
        station_id = station['id']

        # endpoint & url for max temperature
        endpoint = f'data?datasetid=GHCND&datatypeid={datatype}&stationid={station_id}&startdate={startdate}&enddate={enddate}'
        url = base_url + endpoint + '&limit=1000&units=standard'

        # make request for max temperature and min temperature
        response = requests.get(url, headers=header)

        # parse data as JSON
        data = response.json()

        if data:
            # parse temp data (values and dates)
            values = []
            dates = []
            d_and_t = []

            for item in data["results"]:
                values.append(item['value'])
                dates.append(datetime.strptime(item['date'].replace('T00:00:00', ''), '%Y-%m-%d').date())
            d_and_t.append(values)
            d_and_t.append(dates)
            return d_and_t
        else:
            continue


def temperature_dataframe():
    # Get Temperature Data
    print('Enter a city: ex. Charlotte, NC')
    user_input = input()
    print('Enter a start date: (use YYYY-MM-DD format)')
    startdate = input()
    print('Enter an end date: (use YYYY-MM-DD format)')
    enddate = input()
    max_temps = city_weather('TMAX', user_input, startdate, enddate)
    max_temps = max_temps[0]
    min_temps = city_weather('TMIN', user_input, startdate, enddate)
    dates = min_temps[1]
    min_temps = min_temps[0]

    # data to be returned to user
    data = []

    for d in dates:
        data.append({'date': d})

    index = 0
    for i in data:
        i.update({'max_temp': max_temps[index]})
        i.update({'min_temp': min_temps[index]})
        index += 1

    # make dataframe
    df = pd.DataFrame(data)

    print(f'Weather in {user_input} from {startdate} to {enddate}:')
    print(df)

    return df


def main():
    is_using_system = True
    while is_using_system:
        # generate UI
        print('---------------')
        print('    WELCOME ')
        print('---------------')

        # menu options
        print('Please select an option')
        print('1. Get Temperature Data')
        print('2. Quit')
        user_input = int(input())

        if user_input == 1:
            temp_df = temperature_dataframe()
            print('Would you like to save your data into the database? (1 = YES, 2 = NO)')
            user_input = int(input())
            if user_input == 1:
                # save to database and file
                engine = create_engine('mysql://root:codio@localhost/weather')
                temp_df.to_sql('temps', con=engine, if_exists='replace', index=False)
                print('Database saved. Goodbye.')
                is_using_system = False
        if user_input == 2:
            is_using_system = False


if __name__ == "__main__":
    main()
