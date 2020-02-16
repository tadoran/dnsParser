from config.config import Configuration
import os
import csv
import time
import concurrent.futures
import queue
from datetime import date
import pandas as pd
import pickle

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
    devices_df = devices_df.rename_axis('Артикул').reset_index().drop_duplicates("Артикул").set_index("Артикул")
    devices_df.columns = ["Наименование", "Категория"]

    # file_shops = parsed_file.shops
    city_shops_df = pd.DataFrame.from_dict(parsed_file.shops, orient='index').join(other=shop_ids_df).reset_index()

    availability_parsed_df = pd.DataFrame.from_dict(parsed_file.availability_by_shops, orient='index')
    availability_df = (
        (availability_parsed_df
            .reset_index()
            .merge(
            right=city_shops_df,
            left_on="level_1",
            right_on="Code",
            suffixes=["_vib", "_shop"]
        )
        )
        [["level_0_vib", "index", "City", "Price", "ProzaPass"]]
    )

    data_df =(
        availability_df
            .groupby(["City", "level_0_vib"])
            .agg({
                "index": ["count", comma_str],
                "Price": "mean",
                "ProzaPass": "mean"
            })
    )

    data_df.reset_index(inplace=True)
    data_df.columns = ["Город", "Артикул", "Кол-во", "Магазины", "Цена", "ProzaPass"]
    data_df.set_index(["Город", "Артикул"], inplace=True)

    # availability_df.reset_index(inplace=True)
    availability_df.columns = ["Артикул", "Магазин", "Описание", "Цена", "ProzaPass", "Колво", "Категория"]
    availability_df.set_index(["Артикул", "Магазин"], inplace=True)


    str_to_print = f"OK - {parsed_file.city_name}\n"
    print(str_to_print, flush=True, end="")
    queue.put([devices_df.to_dict('index'), availability_df.to_dict('index'), data_df.to_dict('index')])


def write_data_to_csv():
    global queue

    print("Writing data")

    # TODO: make date different
    parsing_date = date.today()
    date_str = parsing_date.strftime("%d.%m.%Y")

    models_file = f"./output/models {date_str} df.csv"
    availability_file = f"./output/availability {date_str} df.csv"
    data_file = f"./output/data {date_str} df.csv"
    shops_file = f"./output/shops {date_str} df.csv"

    # Concatenate all present results together
    counter = 0
    while not queue.empty():
        cur_queue_result = queue.get()
        devices = cur_queue_result[0]
        availability = cur_queue_result[1]
        data = cur_queue_result[2]

        if counter == 0:
            devices_full = devices
            availability_full = availability
            data_full = data
        else:
            devices_full.update(devices)
            availability_full.update(availability)
            data_full.update(data)
        counter += 1

    devices_df_full = pd.DataFrame.from_dict(devices_full, orient='index')
    availability_df_full = pd.DataFrame.from_dict(availability_full, orient='index')
    data_df_full = pd.DataFrame.from_dict(data_full, orient='index')

    print("Writing Shops")
    shops_df["date"] = date_str
    shops_df_final = shops_df.set_index("addr_md5").astype(str)
    shops_df_final.drop_duplicates().to_csv(shops_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",")

    print("Writing Models")
    devices_df_full["Дата"] = date_str
    devices_df_full.rename_axis('Артикул').to_csv(models_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",")

    print("Writing Data")
    data_df_full["Дата"] = date_str
    data_df_full.rename_axis(["Город","Артикул"]).reset_index().to_csv(data_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",", index=False)

    print("Writing Availability")
    availability_df_full["Дата"] = date_str
    availability_df_full.to_csv(availability_file, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",")


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

    # dns = DnsWebsite()
    with open('.//web//site.pickle', 'rb') as f:
        dns = pickle.load(f)




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
