
import json
import requests

# load json cache if exist, returns dict
def read_json(cache_file):

    try:
        f = open(cache_file, 'r')
        data = f.read()
        js = json.loads(data)
        f.close()
    except: 
        js = {}

    return js

# encodes dict into JSON format and writes the JSON to file to save the search results
def write_json(cache_file, dict):
    with open(cache_file, "w") as outfile:
        json.dump(dict, outfile)

# get data from cache or API
def get_toppop_cities(cache_file):
    url = 'https://public.opendatasoft.com/api/records/1.0/search/?dataset=geonames-all-cities-with-a-population-1000&q=&rows=1000&sort=population&facet=feature_code&facet=cou_name_en&facet=timezone'
    r = requests.get(url)
    js = json.loads(r.text)

    dat_dict = read_json(cache_file)
    
    counter = 0
    for item in js['records']:
        geoname_id = item['fields']['geoname_id']

        if geoname_id in dat_dict.keys():
            continue
        elif counter < 25: #limit pull 25
            write_json(cache_file, read_json(cache_file))
            with open(cache_file) as f:
                data = json.load(f)

            city_name = item['fields']['name']
            country = item['fields']['cou_name_en']
            population = item['fields']['population']
            latitude = item['fields']['coordinates'][0]
            longitude = item['fields']['coordinates'][1]

            record = {}
            record['city'] = city_name
            record['country'] = country
            record['population'] = population
            record['latitude'] = latitude
            record['longitude'] = longitude

            data[geoname_id] = record

            with open(cache_file, 'w') as f:
                json.dump(data, f)

            counter += 1
        else:
            pass
                
get_toppop_cities('toppop_cities.json')
print(read_json('toppop_cities.json'))

