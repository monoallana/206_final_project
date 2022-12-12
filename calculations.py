# calculations + visualizations file > output visualizations and text file
import matplotlib.pyplot as plt
import sqlite3
import os

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn
############################################################################################################

# creating a txt file to write to
f = open('calculations.txt', 'w')
f.close()

# 1. counting how many most populated cities are in a specific region (cities table/region) (join) (Allana) -- bargraph
def vis_count_region(cur, conn):
    cur.execute('SELECT cities.region_id, regions.region_name FROM cities JOIN regions ON cities.region_id = regions.region_id')
    city_region = cur.fetchall()
    conn.commit()
    # print(city_region)

    # create dictionary of region and count
    data = {}
    for item in city_region:
        region = item[1]
        if region in data:
            data[region] += 1
        else:
            data[region] = 1
    # print(data)

    # writing into calculations file
    f = open('calculations.txt', 'a')
    f.write('This is the number of cities per region based on data from our database of the most populous cities:\n')
    for reg in data:
        f.write(reg + " has " + str(data[reg]) + " of the most populous cities in our database.\n")
    f.write('\n')
    f.close()

    # creating visualization
    regions = list(data.keys())
    counts = list(data.values())

    plt.bar(regions, counts, color=['darkred', 'darkmagenta', 'darkslategray', 'darkgoldenrod', 'darkblue', 'darkslateblue', 'darkcyan', 'darkorange', 'darkgrey', 'darkturquoise'])

    plt.title('Number of Cities per Region\n(based on data from our database of most populous cities)')
    plt.xlabel('Regions')
    plt.ylabel('Number of Cities')
    plt.show()

# 2. counting how many most populated cities are in specific AQI ranges (AQI info) (Rachel)

def vis_amount_of_cities_for_AQI_range(cur, conn):
    cur.execute('SELECT Overall_AQI FROM AQI_AND_COORDINATES')
    overall_aqi_data = cur.fetchall()
    conn.commit()

    #Creating dictionary and adding AQI Ranges as defined by American Lung Association
    number_of_cities_in_AQI_Range = {}
    number_of_cities_in_AQI_Range["Good"] = 0
    number_of_cities_in_AQI_Range["Moderate"] = 0
    number_of_cities_in_AQI_Range["Unhealthy for Sensitive Groups"] = 0
    number_of_cities_in_AQI_Range["Unhealthy"] = 0
    number_of_cities_in_AQI_Range["Very Unhealthy"] = 0
    number_of_cities_in_AQI_Range["Hazardous"] = 0

    for aqi in overall_aqi_data:
        if aqi[0] <= 50:
            number_of_cities_in_AQI_Range["Good"] += 1
        elif aqi[0] <= 100:
            number_of_cities_in_AQI_Range["Moderate"] += 1
        elif aqi[0] <= 150:
            number_of_cities_in_AQI_Range["Unhealthy for Sensitive Groups"] += 1
        elif aqi[0] <= 200:
            number_of_cities_in_AQI_Range["Unhealthy"] += 1
        elif aqi[0] <= 300:
            number_of_cities_in_AQI_Range["Very Unhealthy"] += 1
        elif aqi[0] > 300:
            number_of_cities_in_AQI_Range["Hazardous"] += 1

    # writing aqi range data into the calculations file
    f = open('calculations.txt', 'a')
    f.write('This is the number of cities whose air quality index falls into each category defined by the American Lung Association based on data from our database of the most populous cities:\n')
    for aqi_ALA_range in number_of_cities_in_AQI_Range:
        f.write("There are " + str(number_of_cities_in_AQI_Range[aqi_ALA_range]) + " number of cities in the air quality index category " + aqi_ALA_range + " of the most populous cities in our database.\n")
    f.write('\n')
    f.close()

    # creating visualization
    ALA_Category = list(number_of_cities_in_AQI_Range.keys())
    number_of_cities = list(number_of_cities_in_AQI_Range.values())
    for citynumber in number_of_cities:
        citynumber = int(citynumber)

    plt.pie(number_of_cities, labels = ALA_Category, autopct='%1.1f%%', colors=['green', 'yellow', 'darkorange', 'magenta', 'darkmagenta', 'red'])
    plt.title('Number of Cities in each Air Quality Index Category\n(based on data from our database of most populous cities)')
    plt.xlabel('Air Quality Index Category')
    plt.show()

# 3. average air pollutant concentration per by country (join) (Rachel)

def vis_pollutant_by_country(cur, conn):
    cur.execute('SELECT cities.country, AQI_AND_COORDINATES.carbon_monoxide_concentration FROM cities JOIN AQI_AND_COORDINATES ON cities.geoname_id = AQI_AND_COORDINATES.geoname_id')
    countries_data = cur.fetchall()
    conn.commit()

    # create dictionary and calculations for carbon monoxide
    country_carbon_monoxide_concentration = {}
    number_of_data_points_by_country = {}

    for indv_country_data in countries_data:
        country = indv_country_data[0]
        co2_concentration = indv_country_data[1]
        if country in country_carbon_monoxide_concentration:
            country_carbon_monoxide_concentration[country] += co2_concentration
            number_of_data_points_by_country[country] += 1
        else:
            country_carbon_monoxide_concentration[country] = co2_concentration
            number_of_data_points_by_country[country] = 1

    average_carbon_monoxide_concentration_by_country = {}

    for country in country_carbon_monoxide_concentration:
        average_carbon_monoxide_concentration_by_country[country] = country_carbon_monoxide_concentration[country] / number_of_data_points_by_country[country]

    print(average_carbon_monoxide_concentration_by_country)

    # writing into calculations file
    f = open('calculations.txt', 'a')
    f.write('This is the average carbon monoxide concentration average by country based on data from our database of the most populous cities:\n')
    for country_avg in average_carbon_monoxide_concentration_by_country:
        f.write(country_avg + " has an average carbon monoxide concentration of " + str(average_carbon_monoxide_concentration_by_country[country_avg]) + " based on the most populous cities in our database.\n")
    f.write('\n')
    f.close()

    countries = list(average_carbon_monoxide_concentration_by_country.keys())
    averages = list(average_carbon_monoxide_concentration_by_country.values())

    plt.bar(countries, averages, color=['darkred', 'darkmagenta', 'darkslategray', 'darkgoldenrod', 'darkblue', 'darkslateblue', 'darkcyan', 'darkorange', 'darkgrey', 'darkturquoise'])

    plt.title('Average Carbon Monoxide Concentration per Country\n(based on data from our database of most populous cities)')
    plt.xlabel('Countries')
    plt.ylabel('Carbon Monoxide Concentration')
    plt.xticks(
    rotation=90, 
    horizontalalignment='right',
    fontweight='light',
    fontsize='medium'  
)
    plt.tight_layout()

    plt.show()

# 4. average current AQI/weather per region (join) (Allana) -- bargraph
def vis_avAQI_by_region(cur, conn):
    cur.execute('SELECT cities.region_id, regions.region_name, AQI_AND_COORDINATES.Overall_AQI FROM cities JOIN regions ON cities.region_id = regions.region_id JOIN AQI_AND_COORDINATES ON cities.geoname_id = AQI_AND_COORDINATES.geoname_id')
    aqireg_data = cur.fetchall()
    conn.commit()
    # print(aqireg_data)

    sum_data = {}
    for item in aqireg_data:
        region = item[1]
        aqi = item[2]
        if region in sum_data:

            sum_data[region][0] += 1
            sum_data[region][1] += aqi
        else:
            sum_data[region] = [1, aqi]
    
    results = {}
    for item in sum_data:
        city_count = sum_data[item][0]
        aqi_sum = sum_data[item][1]
        results[item] = aqi_sum / city_count
    # print(results)

    # writing into calculations file
    f = open('calculations.txt', 'a')
    f.write('This is the average AQI per region based on data from our database of the most populous cities:\n')
    for reg in results:
        f.write(reg + "'s average AQI is " + str(results[reg]) + " based on its most populous cities.\n")
    f.write('\n')
    f.close()

    # creating visualization
    regions = list(results.keys())
    counts = list(results.values())

    plt.bar(regions, counts, color=['darkred', 'darkmagenta', 'darkslategray', 'darkgoldenrod', 'darkblue', 'darkslateblue', 'darkcyan', 'darkorange', 'darkgrey', 'darkturquoise'])

    plt.title('Average AQI per Region\n(based on data from our database of most populous cities)')
    plt.xlabel('Regions')
    plt.ylabel('AQI')
    plt.show()


# 5. just for fun > population vs AQI (Allana) -- scatterplot
def vis_pop_vs_aqi(cur, conn):
    cur.execute('SELECT cities.population, AQI_AND_COORDINATES.Overall_AQI FROM cities JOIN AQI_AND_COORDINATES ON cities.geoname_id = AQI_AND_COORDINATES.geoname_id')
    popaqi_data = cur.fetchall()
    conn.commit()

    pop_list = []
    aqi_list = []
    for item in popaqi_data:
        pop_list.append(item[0])
        aqi_list.append(item[1])

    plt.figure()
    plt.scatter(pop_list, aqi_list, c='dodgerblue', alpha=0.5)

    plt.xticks()
    plt.title('Population in a City vs. AQI')
    plt.xlabel('Population of City')
    plt.ylabel('AQI')
    plt.tight_layout()
    plt.show()

#Extra to make sure we use weather table (Rachel)
def vis_weather(cur, conn):
    cur.execute('SELECT temperature_f, humidity FROM WEATHER')
    weather_data = cur.fetchall()
    conn.commit()

    temp_list = []
    humidity_list = []

    for item in weather_data:
        temp_list.append(item[0])
        humidity_list.append(item[1])

    plt.figure()
    plt.scatter(temp_list, humidity_list, c='red', alpha=0.5)

    plt.xticks()
    plt.title('Temperature in a City vs. Humidity')
    plt.xlabel('Current Temperature in City')
    plt.ylabel('Current Humidity in City')
    plt.tight_layout()
    plt.show()

# run the show
cur, conn = setUpDatabase('TopCityAQI.db')

vis_count_region(cur, conn)
vis_avAQI_by_region(cur, conn)
vis_pop_vs_aqi(cur, conn)
vis_amount_of_cities_for_AQI_range(cur, conn)
vis_pollutant_by_country(cur, conn)
vis_weather(cur, conn)