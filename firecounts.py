import psycopg2
import csv
import os

#자료 위치
FILEPATH =  os.path.join(os.getcwd(), '시도별_산불발생_현황_20230407150110.csv')

#시도별_산불발생_현황csv파일 불러오기
year_fire = []
with open(FILEPATH, 'r') as f:
    data= csv.reader(f)
    for row in data:
        year_fire.append(row)

    

#데이터 입력을 위한 커서 설정
conn =  psycopg2.connect(
    host = "localhost",
    database = "section4_PR",
    user = "postgres",
    password = ""
)
cur = conn.cursor()


#지역별 아이디 지정을 위한 지역 추출 함수
def region_id():
    x = set()
    for i in range(len(year_fire[0])):
        x.add(year_fire[2][i])
        y = sorted(x, reverse=False)
    y.remove("계")
    y.remove('시점')
    return y

#지역별 연도에 따른 화재 발생 건수 추출 함수
def year_fire_upload(region):
    x = []
    for i in range(len(year_fire)):
        x.append(year_fire[i][region])
    return x

#table reset 함수 정의
def reset_table(tablename):
    return cur.execute(f"DROP TABLE IF EXISTS {tablename};")

#region_id 쿼리문 작성
create_region_id_data ="""CREATE TABLE region(
region_id       SERIAL PRIMARY KEY,
region_name     VARCHAR NOT NULL
);
"""

year_list = list(range(2001, 2022))

#테이블 생성 쿼리문
def create_firecounts_table(region_name):
    sql =f"""CREATE TABLE {region_name}(
    year       INTEGER NOT NULL PRIMARY KEY,
    firecount  INTEGER,
    region_id  INTEGER NOT NULL
    );"""
    cur.execute(sql)
    cur.execute(f"SELECT region_id FROM region WHERE region_name = '{region_name}';")
    region_id = cur.fetchone()[0]
    for year in range(2001, 2022):
        cur.execute(f"INSERT INTO {region_name} (year, region_id) VALUES ({year}, {region_id});")

#쿼리문 실행
region_list = region_id()
reset_table('region')
cur.execute(create_region_id_data)
for region_name in region_list:
    cur.execute(f"""INSERT INTO region (region_name) VALUES ('{region_name}');""")
    reset_table(region_name)
    create_firecounts_table(region_name)

symbol = '-'
sejong = [0 if x == symbol else x for x in year_fire_upload(9)]
jeju = [0 if x == symbol else x for x in year_fire_upload(18)]

for i in range(len(year_list)):
    cur.execute(f"UPDATE 강원 SET firecount = '{year_fire_upload(11)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 경기 SET firecount = '{year_fire_upload(10)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 서울 SET firecount = '{year_fire_upload(2)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 부산 SET firecount = '{year_fire_upload(3)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 대구 SET firecount = '{year_fire_upload(4)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 인천 SET firecount = '{year_fire_upload(5)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 광주 SET firecount = '{year_fire_upload(6)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 대전 SET firecount = '{year_fire_upload(7)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 울산 SET firecount = '{year_fire_upload(8)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 세종 SET firecount = '{sejong[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 충북 SET firecount = '{year_fire_upload(12)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 충남 SET firecount = '{year_fire_upload(13)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 전북 SET firecount = '{year_fire_upload(14)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 전남 SET firecount = '{year_fire_upload(15)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 경북 SET firecount = '{year_fire_upload(16)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 경남 SET firecount = '{year_fire_upload(17)[3:][i]}' WHERE year = {i+2001};")
    cur.execute(f"UPDATE 제주 SET firecount = '{jeju[3:][i]}' WHERE year = {i+2001};")


conn.commit()
conn.close()