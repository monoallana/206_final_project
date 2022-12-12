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

# 3. average air pollutant concentration per by country (join) (Rachel)

# 4. average current AQI/weather per region (join) (Allana)
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

# run the show
cur, conn = setUpDatabase('TopCityAQI.db')

# vis_pop_vs_aqi(cur, conn)
# vis_count_region(cur, conn)
vis_avAQI_by_region(cur, conn)
