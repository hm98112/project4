
from firecounts import reset_table
from pymongo import MongoClient
import psycopg2



HOST = 'cluster0.lfthzlr.mongodb.net'
USER = 'hm98112'
PASSWORD = ''
DATABASE_NAME = 'project4'
COLLECTION_NAME = 'fire2'
MONGO_URI = f"mongodb+srv://{USER}:{PASSWORD}@{HOST}/{DATABASE_NAME}?retryWrites=true&w=majority"


client = MongoClient(MONGO_URI)
database = client[DATABASE_NAME]
collection = database[COLLECTION_NAME]
dict_data = collection.find()




conn =  psycopg2.connect(
    host = "localhost",
    database = "section4_PR",
    user = "postgres",
    password = ""
)
cur = conn.cursor()
#cur.execute(f"DROP TABLE IF EXISTS fire_detail2;")

create_fire_detail2 ="""CREATE TABLE fire_detail2(
ocurdt           FLOAT,
ocurdo           VARCHAR,
tempavg         FLOAT,
humidrel        FLOAT,
windavg         FLOAT,
raindays        FLOAT,
dmgarea         FLOAT
);"""


#cur.execute(create_fire_detail2)

for i in range(883, len(dict_data[0]['ocurdt'])):
    y = {'ocurdt': dict_data[0]['ocurdt'][i],
        'ocurdo': dict_data[0]['ocurdo'][i],
        'tempavg': dict_data[0]['tempavg'][i],
        'humidrel': dict_data[0]['humidrel'][i],
        'windavg': dict_data[0]['windavg'][i],
        'raindays': dict_data[0]['raindays'][i],
        'dmgarea': dict_data[0]['dmgarea'][i]}
    cur.execute(f"""INSERT INTO fire_detail2 
                (ocurdt, ocurdo, tempavg, humidrel, 
                windavg, raindays, dmgarea) VALUES ({y['ocurdt']}, '{y['ocurdo']}', 
                {y['tempavg']}, {y['humidrel']}, {y['windavg']}, {y['raindays']}, {y['dmgarea']});""")
    conn.commit()
conn.close()