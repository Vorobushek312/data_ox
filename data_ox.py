import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine, MetaData, Table, String, Integer, Column, DateTime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime, date

try:
    connection = psycopg2.connect(user="postgres", password="12345678")
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    sql_create_database = cursor.execute('create database data_ox')
    cursor.close()
    connection.close()
except:
    pass

username = 'postgres'
password = '12345678'
engine = create_engine("postgresql+psycopg2://" + username + ":" + password + "@127.0.0.1:5432/data_ox")
meta = MetaData()

data_apportmen = Table(
   'data_apportmen', meta, 
   Column('id', Integer, primary_key = True), 
   Column('picture', String), 
   Column('title', String),
   Column('date_post', DateTime(timezone=False)),
   Column('city', String),
   Column('count_bed', String),
   Column('description', String),
   Column('price_valut', String),
   Column('price', String),  
)

meta.create_all(engine)

for i in range(1, 94):
    r = requests.get(f"https://www.kijiji.ca/b-apartments-condos/city-of-toronto/page-{i}/c37l1700273")
    soup = BeautifulSoup(r.text, 'html.parser')
    id_ap = soup.findAll("div", {"class": re.compile("^search-item")})
    for id in id_ap:
        # print(id.get("data-listing-id"))
        try:
            picture = id.find("picture").find("img").get("data-src")    
        except:
            picture = "None"
        title = id.find("div", {"class": "title"}).find("a").text.strip()
        date_post = id.find("span", {"class": "date-posted"}).text
        try:
            date_post = datetime.strptime(date_post, "%d/%m/%Y")
        except:
            date_post = date.today()
        city = id.find("span", {"class": ""}).text.strip()
        count_bed = " ".join(id.find("span", {"class": "bedrooms"}).text.split())
        description = " ".join(id.find("div", {"class": "description"}).text.split())
        if " ".join(id.find("div", {"class" : "price"}).text.split()) != "Please Contact":
            price_valut = " ".join(id.find("div", {"class" : "price"}).text.split())[0]
            price = " ".join(id.find("div", {"class" : "price"}).text.split())[1:]
        else:
            price_valut = "None"
            price = "None"
        ins = data_apportmen.insert().values(picture = picture, title = title, date_post = date_post, \
            city = city, count_bed = count_bed, description = description, price_valut = price_valut, price = price)
        conn = engine.connect()
        result = conn.execute(ins)



