import requests
import numpy as np
import pandas as pd
import json
import sqlalchemy
from sqlalchemy import create_engine
from datetime import date
from datetime import timedelta

# token
token = "SJRNtbHkWTiiNIkoVWBmPtHqWxYIjZJj"
header = {"token": token}

# station id for charlotte douglas international airport station
station_id = "GHCND:USW00013881"

# which data set to get data from
datasetid = "GHCND"

# what kind of data to get, TAVG = temperature avg
datatypeid = "TMAX"

# limit of how much data to recieve
limit = "7"

date_range = f'startdate={date.today() - timedelta(days=14)}&enddate={date.today() - timedelta(days=7)}'

# base url
base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data'
url = f"{base_url}?stationid={station_id}&datasetid={datasetid}&locationid=FIPS:37&datatypeid={datatypeid}&limit={limit}&{date_range}"

# Test url is not empty
# Test response code is 200 (connected)
# Test data is retrieved (returned list not empty)
def get_weather(url):
  # url endpoint
  url = url

  # make request
  r = requests.get(url, headers=header)
  
  # load response as jason
  d = r.json()

  # take results and put into df
  max_temps = d['results']
  
  return max_temps

df = pd.DataFrame(get_weather(url))

# create engine object
engine = create_engine('mysql://root:codio@localhost/weather')
df.to_sql('max_temps', con=engine, if_exists='replace', index=False)