from db import db
from web.cities import get_cities_and_hashes
from web.city_info import get_city_info
from config.config import Configuration
# import sys

config = Configuration()

curCon = db.DatabaseConnection()

all_cities = cityDict = [x["city_name"] for x in curCon.get_all_cities()]
new_cities = [(name, city_hash) for name, city_hash in get_cities_and_hashes().items() if name not in all_cities]
# print(list(new_cities))
# print(curCon.put_new_cities_to_db(new_cities))
# sys.exit()

cityDict = {x['city_name']: x for x in curCon.get_all_cities_wo_filename()}
for city in cityDict.keys():
    city_dns_name = get_city_info(cityDict[city]["city_hash"])
    if city_dns_name:
        city_dns_xls_name = "price-" + city_dns_name + ".xls"
        city_dns_zip_name = "price-" + city_dns_name + ".zip"
        print(city, cityDict[city]["cities_id"], cityDict[city]["city_hash"], city_dns_name, city_dns_xls_name,
              city_dns_zip_name)
        print(curCon.put_dns_names_to_db(city_dns_name, city_dns_xls_name, city_dns_zip_name,
                                         cityDict[city]["cities_id"]))
    else:
        print(city, cityDict[city]["cities_id"], None)
