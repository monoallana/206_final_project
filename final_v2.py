#FINAL PROJECT
#Group Name: Smog Busters!
#Rachel Sondergeld, rsond@umich.edu 
#Allana Tran, allanatt@umich.edu

import json
import requests
import sqlite3
import os

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

###################################################################################################################################################################################

#Creating a table with the lat/long data
def create_lat_long_table(cur, conn):
    # specify primary key
    cur.execute("CREATE TABLE IF NOT EXISTS AQI_AND_COORDINATES (geoname_id INTEGER PRIMARY KEY, latitude INTEGER, longitude INTEGER, Overall_AQI INTEGER, carbon_monoxide_concentration INTEGER, carbon_monoxide_aqi INTEGER, nitrogen_dioxide_concentration INTEGER, nitrogen_dioxide_aqi INTEGER, ozone_concentration INTEGER, ozone_aqi INTEGER, sulphur_dioxide_concentration INTEGER, sulphur_dioxide_aqi INTEGER, PM2_5_concentration INTEGER, PM2_5_aqi INTEGER, PM10_concentration INTEGER, PM10_aqi)") 
    conn.commit()

#The data for this is also being added at the same time as the other table since they both use lat/long
def create_WEATHER_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS WEATHER (geoname_id INTEGER PRIMARY KEY, temperature_f INTEGER, wind_mph INTEGER, condition TEXT, UV INTEGER, humidity INTEGER)")
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
            counter += 1

            #WORKING WITH API 2
            url = "https://air-quality-by-api-ninjas.p.rapidapi.com/v1/airquality"
            querystring = {"lat":latitude,"lon":longitude}
            headers = {
	        "X-RapidAPI-Key": "6db5d5e867mshf4ffc7fbed8576fp151309jsn099bb384efb5",
	        "X-RapidAPI-Host": "air-quality-by-api-ninjas.p.rapidapi.com"}

            #Adding AQI Data
            response = requests.request("GET", url, headers=headers, params=querystring)
            new_response = json.loads(response.text)
            Overall_AQI = int(new_response["overall_aqi"])
            carbon_monoxide_concentration = int(new_response["CO"]["concentration"])
            carbon_monoxide_aqi = int(new_response["CO"]["aqi"])
            nitrogen_dioxide_concentration = int(new_response["NO2"]["concentration"])
            nitrogen_dioxide_aqi = int(new_response["NO2"]["aqi"])
            ozone_concentration = int(new_response["O3"]["concentration"])
            ozone_aqi = int(new_response["O3"]["aqi"])
            sulphur_dioxide_concentration = int(new_response["SO2"]["concentration"])
            sulphur_dioxide_aqi = int(new_response["SO2"]["aqi"])
            PM2_5_concentration = int(new_response["PM2.5"]["concentration"])
            PM2_5_aqi = int(new_response["PM2.5"]["aqi"])
            PM10_concentration = int(new_response["PM10"]["concentration"])
            PM10_aqi = int(new_response["PM10"]["aqi"])

            cur.execute("INSERT OR IGNORE INTO AQI_AND_COORDINATES (geoname_id, latitude, longitude, Overall_AQI, carbon_monoxide_concentration, carbon_monoxide_aqi, nitrogen_dioxide_concentration, nitrogen_dioxide_aqi, ozone_concentration, ozone_aqi, sulphur_dioxide_concentration, sulphur_dioxide_aqi, PM2_5_concentration, PM2_5_aqi, PM10_concentration, PM10_aqi) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (geoname_id, latitude, longitude, Overall_AQI, carbon_monoxide_concentration, carbon_monoxide_aqi, nitrogen_dioxide_concentration, nitrogen_dioxide_aqi, ozone_concentration, ozone_aqi, sulphur_dioxide_concentration, sulphur_dioxide_aqi, PM2_5_concentration, PM2_5_aqi, PM10_concentration, PM10_aqi))
            

            #Working with API 3
            url = "https://weatherapi-com.p.rapidapi.com/current.json"
            querystring = {"q":f"{latitude},{longitude}"}
            headers = {
	        "X-RapidAPI-Key": "6db5d5e867mshf4ffc7fbed8576fp151309jsn099bb384efb5",
	        "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
            }
            weather_response = requests.request("GET", url, headers=headers, params=querystring)
            new_weather_response = json.loads(weather_response.text)
            temperature_f = new_weather_response["current"]["temp_f"]
            wind_mph = new_weather_response["current"]["wind_mph"]
            condition = new_weather_response["current"]["condition"]["text"]
            UV = new_weather_response["current"]["uv"]
            humidity = new_weather_response["current"]["humidity"]

            cur.execute("INSERT OR IGNORE INTO WEATHER (geoname_id, temperature_f, wind_mph, condition, UV, humidity) VALUES (?,?,?,?,?,?)",(geoname_id, temperature_f, wind_mph, condition, UV, humidity))

        else:
            break  
    conn.commit()

##############################################################################################################################################################################################################################################

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

    #AQI and weather tables
    create_lat_long_table(cur, conn)
    create_WEATHER_table(cur, conn)
    add_lat_long_data(rows, get_geoData(),cur, conn)




main()