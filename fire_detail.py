import requests
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient


url = 'http://apis.data.go.kr/1400000/forestStusService/getfirestatsservice'

HOST = 'cluster0.lfthzlr.mongodb.net'
USER = 'hm98112'
PASSWORD = ''
DATABASE_NAME = 'project4'
COLLECTION_NAME = 'fire'
MONGO_URI = f"mongodb+srv://{USER}:{PASSWORD}@{HOST}/{DATABASE_NAME}?retryWrites=true&w=majority"


client = MongoClient(MONGO_URI)
database = client[DATABASE_NAME]
collection = database[COLLECTION_NAME]


for i in range(1,1000):
    time.sleep(1)
    data = {'start_year' : [],
        'start_month' : [],
        'start_day' : [],
        'start_time' : [],
        'start_dayofweek' : [],
        'end_year' : [],
        'end_month' : [],
        'end_day' : [],
        'end_time' : [],
        "location" : [],
        'damage_area' : [],
        'fire_cuase' : []
        }
    response = requests.get(url, params={'serviceKey' : 'ADaZxYW6/u09/F6S0KiZH6b7IRYuRn98Gpk7/nQ/0lWV2htC0SoMMM0paByNik3n0qEw7X4I2fDmYGeXNuyxSg==', 'numOfRows' : '99', 'pageNo' : f'{i}', 'searchStDt' : '20010101', 'searchEdDt' : '20211231' })
    parsed_data = BeautifulSoup(response.content, features="xml")
    items = parsed_data.find_all('item')
    for item in items:
        start_month = item.find('startmonth').text
        start_time = item.find('starttime').text
        start_year = item.find('startyear').text
        start_dayofweek = item.find('startdayofweek').text
        start_day = item.find('startday').text
        location = item.find('locsi').text
        fire_cuase = item.find('firecause').text
        damage_area = item.find('damagearea').text
        end_day = item.find('endday').text
        end_month = item.find('endmonth').text
        end_time = item.find('endtime').text
        end_year = item.find('endyear').text
        data['start_year'].append(start_year)
        data['start_month'].append(start_month)
        data['start_day'].append(start_day)
        data['start_time'].append(start_time)
        data['start_dayofweek'].append(start_dayofweek)
        data['end_year'].append(end_year)
        data['end_month'].append(end_month)
        data['end_day'].append(end_day)
        data['end_time'].append(end_time)
        data['location'].append(location)
        data['damage_area'].append(damage_area)
        data['fire_cuase'].append(fire_cuase)
    collection.insert_one(data)

collection.delete_many({ 'start_month': { '$size': 0 }})