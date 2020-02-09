from config.config import Configuration
import os
import csv
import time
import concurrent.futures
import queue
from datetime import date
import pandas as pd

from files.xls_obj import Dns_parse_file
from web.dns_website_obj import DnsWebsite

def comma_str(lst):
    return ",".join([str(x) for x in lst])

def parse_dns_xls_df(*args):
    # Name of parsed file
    filename = args[0]
    # Categories for import
    categories = args[1]
    queue = args[2]
    shop_ids_df = args[3]

    category_names = [obj.nameGroup for obj in categories]
    category_dict = {obj.nameGroup: obj.Id for obj in categories}

    # File imports
    parsed_file = Dns_parse_file(filename, category_names).parse()


    # file_devices = parsed_file.devices
    devices_df = pd.DataFrame.from_dict(parsed_file.devices, orient='index')

    # file_shops = parsed_file.shops
    city_shops_df = pd.DataFrame.from_dict(parsed_file.shops, orient='index').join(other=shop_ids_df).reset_index()

    availability_df = pd.DataFrame.from_dict(parsed_file.availability_by_shops, orient='index')

    data_df = (
        (availability_df
            .reset_index()
            .merge(
                right=city_shops_df,
                left_on="level_1",
                right_on="Code",
                suffixes=["_vib", "_shop"]
                )
            )
            [["level_0_vib", "index", "City", "Price", "ProzaPass"]]
                .groupby(["City", "level_0_vib"])
                .agg({
                    "index": ["count", comma_str],
                    "Price": "mean",
                    "ProzaPass": "mean"}
                    )
    )

    str_to_print = f"OK - {parsed_file.city_name}\n"
    print(str_to_print, flush=True, end="")
    queue.put([devices_df, availability_df, data_df])

def parse_dns_xls_local(*args):
    # Name of parsed file
    filename = args[0]
    # Categories for import
    categories = args[1]
    queue = args[2]
    # SQL imports
    category_names = [obj.nameGroup for obj in categories]
    category_dict = {obj.nameGroup: obj.Id for obj in categories}

    # File imports
    parsed_file = Dns_parse_file(filename, category_names).parse()
    file_devices = parsed_file.devices
    file_shops = parsed_file.shops
    parse_results = parsed_file.availability_by_shops

    str_to_print = f"OK - {parsed_file.city_name}\n"
    print(str_to_print, flush=True, end="")
    queue.put([file_devices, file_shops, parse_results, category_dict])


def write_data_to_csv():
    global queue

    print("write_data_to_csv iteration starts")

    # shops = {}
    # models = {}
    # availability = {}
    # category_dict = {}
    # lines_wrote = {}


    # TODO: make date different
    parsing_date = date.today()
    date_str = parsing_date.strftime("%d-%m-%Y")

    # Concatenate all present results together
    counter = 0
    while not queue.empty():
        cur_queue_result = queue.get()
        devices_df = cur_queue_result[0]
        availability_df = cur_queue_result[1]
        data_df = cur_queue_result[2]

        if counter == 0:
            devices_df_full = devices_df
            availability_df_full = availability_df
            data_df_full = data_df
        else:
            devices_df_full = devices_df_full.append(devices_df)#.drop_duplicates()
            availability_df_full = availability_df_full.append(availability_df)#.drop_duplicates()
            data_df_full = data_df_full.append(data_df)#.drop_duplicates()

        # shops_encode = {item["Code"]: key for key, item in cur_shops.items()}
        # cur_availability = cur_queue_result[2]
        # cur_category_dict = cur_queue_result[3]
        #
        # models.update(cur_models)
        # shops.update(cur_shops)
        #
        # try:
        #     cur_availability_encoded = {(key[0], shops_encode[key[1]]): item for key, item in cur_availability.items()}
        # except KeyError:
        #     pass
        #
        # availability.update(cur_availability_encoded)
        # category_dict.update(cur_category_dict)
        counter += 1

    models_file = f"./output/models {date_str} df.csv"
    availability_file = f"./output/availability {date_str} df.csv"
    data_file = f"./output/data {date_str} df.csv"

    shops_file = f"./output/shops {date_str} df.csv"


    availability_df_full["date"] = date_str
    availability_df_full.reset_index().drop_duplicates().to_csv(availability_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",")


    data_df_full["date"] = date_str
    data_df_full.reset_index().drop_duplicates().to_csv(data_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",")


    shops_df["date"] = date_str
    shops_df.reset_index().drop_duplicates().to_csv(shops_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",")



    devices_df_full["date"] = date_str
    devices_df_full.reset_index().drop_duplicates().to_csv(models_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",")
    # print(f"write_data_to_csv collected {counter} file results. Processing.")
    # pd.DataFrame.to_csv()


    #
    # ### DEVICES
    # # lines = [[str(article), details["Descr"], details["Category"]] for article, details in models.items()]
    # # with open(models_file, "a", errors="ignore", newline='') as mf:
    # #     # If there are new shops in file - save them to file
    # #     if len(lines) > 0:
    # #         # mf.writelines(lines)
    # #         writer = csv.writer(mf, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
    # #         for line in lines:
    # #             writer.writerow(line)
    # #         lines_wrote["Devices"] = len(lines)
    #
    # ### SHOPS
    # lines = [[MD5_hash, details["Name"], details["Phone"], details["WorkTime"], details["Address"], details["City"]] for
    #          MD5_hash, details in shops.items()]
    # # print(lines)
    # with open(shops_file, "a", errors="ignore", newline='', encoding='utf-8') as sf:
    #     # If there are new shops in file - save them to file
    #     if len(lines) > 0:
    #         # sf.writelines(lines)
    #         writer = csv.writer(sf, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, )
    #         for num, line in enumerate(lines):
    #             # print(line + [num+1])
    #             writer.writerow(line + [num + 1])
    #         lines_wrote["Shops"] = len(lines)
    #
    # ### AVAILABILITY
    # lines = [[str(key[0]), str(value["Price"]), str(value["ProzaPass"]), str(key[1]), str(parsing_date)] for
    #          key, value in availability.items()]
    # with open(availability_file, "a", errors="ignore", newline='') as af:
    #     if len(lines) > 0:
    #         writer = csv.writer(af, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
    #         for line in lines:
    #             writer.writerow(line)
    #
    #         lines_wrote["Available"] = len(lines)
    #
    # availability_filename = availability_file
    # availability_headers = ["Article", "Price", "ProzaPass", "Shop", "Date"]
    # availability = pd.read_csv(
    #     availability_filename,
    #     sep=";",
    #     names=availability_headers,
    #     parse_dates=True
    # )
    # shops_filename = shops_file
    # shops_headers = ["shop_id", "shop_name", "shop_phone", "shop_worktime", "shop_address", "shop_city", "shop_num"]
    # shops = pd.read_csv(shops_filename, sep=";", names=shops_headers, parse_dates=True, encoding="utf-8")
    # shops.head()
    #
    # data = (
    #     availability
    #         .merge(right=shops, how="left", left_on="Shop", right_on="shop_id")
    #         .groupby(["shop_city", "Article"])
    #         .agg({
    #         "Price": min,
    #         "ProzaPass": min,
    #         "shop_num": [comma_str, "count"],
    #         "Date": min
    #     })
    #     .reset_index()
    # )
    #
    # data.columns = ["city", "article", "price", "ProzaPass", "shops", "shops_count", "date"]
    # data.date = pd.to_datetime(data.date)
    #
    # data.to_csv(
    #     data_file,
    #     sep=";",
    #     header=True,
    #     index=False,
    #     quoting=1,  # QUOTE_ALL
    #     date_format="%d.%m.%Y"  # 08.01.2020
    # )

    # processed = ", ".join(key + " - " + str(value) for key, value in lines_wrote.items())
    # print("write_data_to_csv Processed: " + processed)


class Category:
    def __init__(self, name, id):
        self.nameGroup = name
        self.Id = id

    def __str__(self):
        return f"({self.Id}) {self.nameGroup}"


if __name__ == '__main__':
    t1 = time.perf_counter()
    categories = [
        "Варочные панели газовые", "Варочные панели электрические", "Встраиваемые посудомоечные машины",
        "Духовые шкафы электрические", "Посудомоечные машины", "Стиральные машины", "Холодильники", "Вытяжки",
        "Блендеры погружные", "Блендеры стационарные", "Грили и раклетницы",
        "Измельчители", "Кофеварки капельные",
        "Кофемашины автоматические", "Кофемашины капсульные", "Кофемолки",
        "Кухонные комбайны", "Микроволновые печи", "Миксеры", "Мультиварки", "Мясорубки", "Соковыжималки", "Чайники",
        "Электрочайники",
        "Гладильные доски", "Гладильные системы", "Мешки-пылесборники", "Парогенераторы", "Пылесосы", "Утюги"
    ]

    config = Configuration()
    xls_files_directory = config.folders["xls_folder"]

    dns = DnsWebsite()
    shops_df = dns.shops.reset_index(drop=True).reset_index()
    shop_ids_df = shops_df[["index", "addr_md5"]].set_index("addr_md5")

    files = [os.path.join(xls_files_directory, file) for file in os.listdir(xls_files_directory) if
             file.endswith(".xls")][:15]
    categories = [Category(name, i + 1) for i, name in enumerate(categories)]
    queue = queue.Queue()

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        # Starting threads for file parsing
        results = [executor.submit(parse_dns_xls_df, file, categories, queue, shop_ids_df) for file in files]

    concurrent.futures.wait(results, return_when=concurrent.futures.ALL_COMPLETED)
    # queue.put("EXIT")
    write_data_to_csv()

    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')