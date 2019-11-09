from db import db
from web.city_info import get_city_info
from config.config import Configuration

config = Configuration()

curCon = db.DatabaseConnection()

cityDict = {x['city_name']: x for x in curCon.get_all_cities_wo_filename()}
for city in cityDict.keys():
    city_dns_name = get_city_info(cityDict[city]["city_hash"])
    if city_dns_name:
        city_dns_xls_name = "price-" + city_dns_name + ".xls"
        city_dns_zip_name = "price-" + city_dns_name + ".zip"
        print(city,  cityDict[city]["cities_id"], cityDict[city]["city_hash"], city_dns_name, city_dns_xls_name, city_dns_zip_name)
        print(curCon.put_dns_names_to_db(city_dns_name, city_dns_xls_name, city_dns_zip_name, cityDict[city]["cities_id"]))
    else:
        print(city, cityDict[city]["cities_id"], None)
