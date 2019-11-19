from db import db
from config.config import Configuration
import os
import concurrent.futures
import csv

from files.xls import parse_dns_xls


def parse_dns_xls_local(filename):
    local_name = os.path.basename(filename)
    parse_results = parse_dns_xls(os.path.normpath(filename))

    # parse_results
    # list[city_shops]: Dict(MD5("Address") : {"Code", "Name", "Phone", "WorkTime","Address" })
    # list[data_retrieved] = {Артикул: Dict("Descr", "Price", "ProzaPass" ,"AvailableIn" ,"AvailableCount" ,
    #                         "Category"})

    shops_dic = parse_results[0]
    city_ids = curCon.get_shops_id_by_md5(shops_dic.keys())
    ids_dic = {values["MD5"]: values["id"] for values in city_ids}
    ids_absent = [md5_hash for md5_hash in shops_dic.keys() if ids_dic.get(md5_hash, None) is None]
    for md5_hash in ids_absent:
        insert_tuple = (
                        md5_hash,
                        shops_dic[md5_hash]["Name"],
                        shops_dic[md5_hash]["Phone"],
                        shops_dic[md5_hash]["WorkTime"],
                        shops_dic[md5_hash]["Address"]
                        )
        insert_result = curCon.put_shops_to_db(insert_tuple)
        print(f"Putting {insert_tuple} to database. result: {int(insert_result)}")

    # TODO: Clear this mess!
    city_ids = curCon.get_shops_id_by_md5(shops_dic.keys())
    ids_dic = {values["MD5"]: values["id"] for values in city_ids}

    shop_code_to_id = {details["Code"]: ids_dic[md5_hash] for md5_hash, details in shops_dic.items()}
    print(f"shop_code_to_id: {shop_code_to_id}")

    articles_dic = shops_dic = parse_results[1]
    for article_key, article_details in articles_dic.items():
        print(f"{article_details}")
        for shop in article_details["AvailableIn"]:
            # print(f"Shop: {shop}, {shop_code_to_id[shop]}")
            # `device_article`, `price`, `prozaPass`, `shop`
            availability_insert_tuple = (article_key,
                                         article_details["Price"],
                                         article_details["ProzaPass"],
                                         shop_code_to_id[shop],
                                         "2019-11-16"
                                         )

            insert_availability_result = curCon.put_availability_to_db(availability_insert_tuple)
            print(f"Putting {availability_insert_tuple} to database. result: {insert_availability_result}")




# BIG STINKY MESS
    # shops_list = [[local_name, key] + [str(val) for val in value.values()] for key, value in parse_results[0].items()]
    # fieldnames = ["File", "MD5", "Key", "Descr", "Phone", "WorkTime", "Address"]
    # # with open("shops.csv", "a", errors="ignore", newline='') as f:
    # #     writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
    # #     for line in shops_list:
    # #         writer.writerow(line)
    #
    # md5_vals = {shop[1]: shop[2] for shop in shops_list}
    # print(md5_vals)
    # city_ids = curCon.get_shops_id_by_md5(md5_vals)
    # ids_dic = {values["MD5"]: values["id"] for values in city_ids}
    # ids_absent = {key: val for key, val in md5_vals.items() if ids_dic.get(val, None) is None}
    # print(f"Existing shops: {ids_dic}\nAbsent shops: {ids_absent}")
    # for md5_hash, city in ids_absent.items():
    #     insert_result = curCon.put_new_cities_to_db((city, md5_hash))
    #     print(f"Putting {(city, md5_hash)} to database. result: {int(insert_result)}")
    # city_ids = curCon.get_shops_id_by_md5(md5_vals)
    # ids_dic = {values["MD5"]: values["id"] for values in city_ids}
    # print(f"going to shop ids again. {ids_dic}")
    #
    # vib_list = [[local_name, str(key)] + [str(val) for val in value.values()] for key, value in
    #             parse_results[1].items()]
    # print(vib_list)
    # # with open("data.csv", "a", errors="ignore", newline='') as f:
    # #     writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
    # #     for line in vib_list:
    # #         writer.writerow(line)


config = Configuration()
curCon = db.DatabaseConnection()

xls_files_directory = config.folders["xls_folder"]
files = [xls_files_directory + "\\" + file for file in os.listdir(xls_files_directory) if file.endswith(".xls")][10:13]

# with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
#     executor.map(parse_dns_xls_local, files, timeout=60)
#
for file in files:
    # print(os.path.abspath(file))
    a = parse_dns_xls_local(os.path.normpath(file))
# #     txt = str(a)
# #     with open("log.txt", "a+") as f:
# #         json.dump(txt, f)
# #         # f.write(txt)
# # curCon = db.DatabaseConnection()
