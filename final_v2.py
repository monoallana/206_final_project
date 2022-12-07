
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
    


main()