import datetime
import time
import requests

API_key = "03c1d6d6b7dde6541325df595714646f"

lon = 37.541
lat = 126.986


dt1 = int(datetime.datetime(2000, 12, 31, 15, 0).timestamp())
dt2 = int(datetime.datetime(2021, 12, 31, 15, 0).timestamp())
x = []
for tm in range(dt1, dt2, 10800):
    time.sleep(1)
    url = f'https://history.openweathermap.org/data/3.0/history/timemachine?lat={lat}&lon={lon}&dt={tm}&appid={API_key}'
    response = requests.get(url)