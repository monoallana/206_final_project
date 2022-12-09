#FINAL PROJECT
#Group Name: Smog Busters
#Rachel Sondergeld, rsond@umich.edu 
#Allana Tran, allanatt@umich.edu

import json
import requests
import sqlite3
import os
import time

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# get geo API data 
def get_geoData():
    url = 'https://public.opendatasoft.com/api/records/1.0/search/?dataset=geonames-all-cities-with-a-population-1000&q=&rows=200&sort=population&facet=feature_code&facet=cou_name_en&facet=timezone'
    r = requests.get(url)
    dat = json.loads(r.text)

    return dat

# CREATE TABLE FOR CITIES INFORMATION IN DATABASE AND ADD INFO
def create_cityinfo_table(cur, conn):
    # specify primary key
    cur.execute("CREATE TABLE IF NOT EXISTS cities (geoname_id INTEGER PRIMARY KEY, city_name TEXT, country TEXT, population INTEGER, region_id INTEGER)") 
    conn.commit()

# get data from API
def add_toppop_cities(cur_row, data, cur, conn):
    # PUT THE DATA IN
    info = data
    regions = []
    counter = 0


    # make region list
    for item in info['facet_groups'][2]['facets']:
        region_name = item['name']
        regions.append(region_name)

    # get 25 and add to table
    for item in info['records'][cur_row:]:
        if counter < 25:
            geoname_id = int(item['fields']['geoname_id'])
            city_name = item['fields']['name']
            country = item['fields']['cou_name_en']
            population = int(item['fields']['population'])
            region = (item['fields']['timezone']).split('/')[0]
            region_id = int(regions.index(region))

            counter += 1
            cur.execute("INSERT OR IGNORE INTO cities (geoname_id, city_name, country, population, region_id) VALUES (?,?,?,?,?)", (geoname_id, city_name, country, population, region_id))
        else:
            break
    conn.commit()

# Creating a table for region IDs
def create_region_table(cur, conn):
    # specify primary key
    cur.execute("CREATE TABLE IF NOT EXISTS regions (region_id INTEGER PRIMARY KEY, region_name TEXT)") 
    conn.commit()

def add_region_info(data, cur, conn):
    info = data
    counter = 0
    # make region list
    for item in info['facet_groups'][2]['facets']:
        region_id = counter
        region_name = item['name']
        counter += 1
        cur.execute("INSERT OR IGNORE INTO regions (region_id, region_name) VALUES (?,?)", (region_id, region_name))

    conn.commit()

#########################################

#Creating a table with the lat/long data
def create_lat_long_table(cur, conn):
    # specify primary key
    cur.execute("CREATE TABLE IF NOT EXISTS AQI_AND_COORDINATES (geoname_id INTEGER PRIMARY KEY, latitude INTEGER, longitude INTEGER)") 
    conn.commit()

#Adding Latitude and Longitude to a new table
def add_lat_long_data(cur_row, data, cur, conn):
    # PUT THE DATA IN
    info = data
    counter = 0

    # get 25 and add to table
    for item in info['records'][cur_row:]:
        if counter < 25:
            geoname_id = int(item['fields']['geoname_id'])
            latitude = item['fields']['coordinates'][0]
            longitude = item['fields']['coordinates'][1]

            ###
    # for i in latitude_list:
    #     lat = latitude_list[i]
    #     for j in longitude_list:
    #         long = longitude_list[i]
    #         url = 


    #         ###

    #         ##new
            url = f"http://api.airvisual.com/v2/nearest_city?lat={latitude}&lon={longitude}&key=5c55caf7-4f01-424c-a8ba-a5b474141637"
            payload={}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            time.sleep(10)
            print(json.loads(response.text))
    #         counter += 1
            #response['data']['']
            cur.execute("INSERT OR IGNORE INTO AQI_AND_COORDINATES (geoname_id, latitude, longitude) VALUES (?,?,?)", (geoname_id, latitude, longitude))
        else:
            break

    conn.commit()


#Getting AQI Data from API 2
# def get_aqi_data():
#     for row in [lat/long column in the database]:
#         url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={long}&key=5c55caf7-4f01-424c-a8ba-a5b474141637"
#         payload={}
#         headers = {}
#         response = requests.request("GET", url, headers=headers, data=payload)
#     print(response.text)

# def add_AQI_data(cur, conn):
#     cur.execute('INSERT OR IGNORE INTO AQI_AND_COORDINATES (geoname_id INTEGER, Latitude INTEGER, Longitude INTEGER, AQI INTEGER)')


#########################################
def main():
    # SETUP DATABASE
    cur, conn = setUpDatabase('TopCityAQI.db')
    
    # CREATE TABLES
    create_cityinfo_table(cur, conn)
    create_region_table(cur, conn)

    # GETTING ROW NUMBERS
    cur.execute('''
    SELECT 
    ROW_NUMBER() OVER (
            ORDER BY geoname_id
    ) row_num
    FROM cities
    ''')
    rows = len(cur.fetchall())

    # ADDING DATA
    add_toppop_cities(rows, get_geoData(),cur, conn)
    add_region_info(get_geoData(), cur, conn)

    #testing
    #get_aqi_data()
    create_lat_long_table(cur, conn)
    add_lat_long_data(rows, get_geoData(),cur, conn)


main()