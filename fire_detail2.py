import requests
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient


url = 'http://openapi.forest.go.kr/openapi/service/forestDisasterService/frstFireOpenAPI'

HOST = 'cluster0.lfthzlr.mongodb.net'
USER = 'hm98112'
PASSWORD = ''
DATABASE_NAME = 'project4'
COLLECTION_NAME = 'fire2'
MONGO_URI = f"mongodb+srv://{USER}:{PASSWORD}@{HOST}/{DATABASE_NAME}?retryWrites=true&w=majority"


client = MongoClient(MONGO_URI)
database = client[DATABASE_NAME]
collection = database[COLLECTION_NAME]


for i in range(1,1000):
    time.sleep(1)
    data = {
        'ocurdt'	: [],
        'ocurdo'	: [],
        'extingdt'	: [],
        'exintgtm'	: [],
        'riskavg' : [],
        'riskmax' : [],
        'tempavg' : [],
        'humidcurr'	: [],
        'humidrel'	: [],
        'humidmin'	: [],
        'windmax'	: [],
        'windavg'	: [],
        'raindays'	: [],
        'rainamount': [],
        'dmgarea' : [],
        'ocurcause' : []        
        }
    response = requests.get(url, params={'serviceKey' : 'ADaZxYW6/u09/F6S0KiZH6b7IRYuRn98Gpk7/nQ/0lWV2htC0SoMMM0paByNik3n0qEw7X4I2fDmYGeXNuyxSg==', 'numOfRows' : '9999', 'pageNo' : f'{i}', 'searchStDt' : '20010101', 'searchEdDt' : '20211231' })
    parsed_data = BeautifulSoup(response.content, features="xml")
    items = parsed_data.find_all('item')
    for item in items:
        ocurdt = item.find('ocurdt').text
        ocurdo = item.find('ocurdo').text
        extingdt = item.find('extingdt').text
        exintgtm = item.find('exintgtm').text
        riskavg = item.find('riskavg').text
        riskmax = item.find('riskmax').text
        tempavg = item.find('tempavg').text
        humidcurr = item.find('humidcurr').text
        humidrel = item.find('humidrel').text
        humidmin = item.find('humidmin').text
        windmax = item.find('windmax').text
        windavg = item.find('windavg').text
        raindays = item.find('raindays').text
        rainamount = item.find('rainamount').text
        dmgarea = item.find('dmgarea').text
        ocurcause = item.find('ocurcause').text
        
        data['ocurdt'].append(ocurdt)
        data['ocurdo'].append(ocurdo)
        data['extingdt'].append(extingdt)
        data['exintgtm'].append(exintgtm)
        data['riskavg'].append(riskavg)
        data['riskmax'].append(riskmax)
        data['tempavg'].append(tempavg)
        data['humidcurr'].append(humidcurr)
        data['humidrel'].append(humidrel)
        data['humidmin'].append(humidmin)
        data['windmax'].append(windmax)
        data['windavg'].append(windavg)
        data['raindays'].append(raindays)
        data['rainamount'].append(rainamount)
        data['dmgarea'].append(dmgarea)
        data['ocurcause'].append(ocurcause)
    collection.insert_one(data)

collection.delete_many({ 'ocurdt': { '$size': 0 }})