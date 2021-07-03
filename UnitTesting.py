import unittest
from TestingActivity import get_weather
from datetime import date
from datetime import timedelta
from json import JSONDecodeError

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

class TestFileName(unittest.TestCase):
    def test_get_weather(self):
      # test is list is not empty
        self.assertTrue(get_weather(url))


if __name__ == '__main__':
    unittest.main()