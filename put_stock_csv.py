from config.config import Configuration
import os
import csv
import concurrent.futures
import queue
import time

from datetime import date
# from files.xls import parse_dns_xls
from files.xls_obj import Dns_parse_file


# from db.models import Device, Shop, Availability
# from db.define_sql import get_session, get_categories


def parse_dns_xls_local(*args):
    # Name of parsed file
    filename = args[0]
    # Categories for import
    categories = args[1]
    queue = args[2]
    # print(f"Request for parsing {filename}")
    # print(".", end=" ")
    # SQL imports
    category_names = [obj.nameGroup for obj in categories]
    category_dict = {obj.nameGroup: obj.Id for obj in categories}

    # File imports
    parsed_file = Dns_parse_file(filename, category_names).parse()
    file_devices = parsed_file.devices
    file_shops = parsed_file.shops
    parse_results = parsed_file.availability_by_shops

    str_to_print = f"OK - {parsed_file.city_name}\n"
    print(str_to_print, flush = True, end="")
    queue.put([file_devices, file_shops, parse_results, category_dict])


def CSV_PUSH(endless=True):
    global queue
    max_files_proccesed = 15

    print("CSV_PUSH iteration starts")

    # Main loop
    while True:
        shops = {}
        models = {}
        availability = {}
        category_dict = {}
        lines_wrote = {}

        # Concatenate all present results together
        counter = 0
        while not queue.empty(): #and counter < 20:
            # print(f"There are {len(list(queue.queue))} items in queue now.")
            cur_queue_result = queue.get()
            if cur_queue_result == "EXIT":
                print("CSV_PUSH: I see EXIT element. Closing soon.")
                if counter == 0:
                    return
                else:
                    endless = False
                    continue
            cur_models = cur_queue_result[0]
            cur_shops = cur_queue_result[1]

            shops_encode = {item["Code"]: key for key, item in cur_shops.items()}
            # print(shops_encode)

            cur_availability = cur_queue_result[2]
            cur_category_dict = cur_queue_result[3]

            models.update(cur_models)
            shops.update(cur_shops)

            try:
                cur_availability_encoded = {(key[0], shops_encode[key[1]]): item for key, item in cur_availability.items()}
            except KeyError:
                pass

            availability.update(cur_availability_encoded)
            category_dict.update(cur_category_dict)
            counter += 1

        # if counter == 0:
        #     time.sleep(1)
        # # elif counter >= max_files_proccesed:
        # #     print(f"Collected maximum results for a commit ({max_files_proccesed}). Pushing.")
        # #     break
        # else:
        #     print(f"CSV_PUSH collected {counter} file results. Processing.")


        print(f"CSV_PUSH collected {counter} file results. Processing.")

        # TODO: make date different
        parsing_date = date.today()
        date_str = parsing_date.strftime("%d-%m-%Y")

        models_file = f"./output/models {date_str}.csv"
        shops_file = f"./output/shops {date_str}.csv"
        availability_file = f"./output/availability {date_str}.csv"

        ### DEVICES
        lines = [[str(article), details["Descr"], details["Category"]] for article, details in models.items()]
        with open(models_file, "a", errors="ignore", newline='') as mf:
            # If there are new shops in file - save them to file
            if len(lines) > 0:
                # mf.writelines(lines)
                writer = csv.writer(mf, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
                for line in lines:
                    writer.writerow(line)
                lines_wrote["Devices"] = len(lines)

        ### SHOPS
        lines = [[MD5_hash, details["Name"], details["Phone"], details["WorkTime"], details["Address"], details["City"]] for
                 MD5_hash, details in shops.items()]
        # print(lines)
        with open(shops_file, "a", errors="ignore", newline='', encoding='utf-8') as sf:
            # If there are new shops in file - save them to file
            if len(lines) > 0:
                # sf.writelines(lines)
                writer = csv.writer(sf, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, )
                for num, line in enumerate(lines):
                    # print(line + [num+1])
                    writer.writerow(line + [num+1])
                lines_wrote["Shops"] = len(lines)

        ### AVAILABILITY
        lines = [[str(key[0]), str(value["Price"]), str(value["ProzaPass"]), str(key[1]), str(parsing_date)] for
                 key, value in availability.items()]
        with open(availability_file, "a", errors="ignore", newline='') as af:
            if len(lines) > 0:
                writer = csv.writer(af, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
                for line in lines:
                    writer.writerow(line)

                lines_wrote["Available"] = len(lines)

        if endless == False:
            print("CSV_PUSH: Done.")
            print("CSV_PUSH: Bye-bye!")
            break

        processed = ", ".join(key + " - " + str(value) for key, value in lines_wrote.items())
        print("CSV_PUSH Processed: " + processed)
        # print("CSV_PUSH going to next lap.")


class Category:
    def __init__(self, name, id):
        self.nameGroup = name
        self.Id = id

    def __str__(self):
        return f"({self.Id}) {self.nameGroup}"

if __name__ == '__main__':
    categories = [
        "Варочные панели газовые", "Варочные панели электрические", "Встраиваемые посудомоечные машины",
        "Духовые шкафы электрические", "Посудомоечные машины", "Стиральные машины", "Холодильники", "Вытяжки",
        "Блендеры погружные", "Блендеры стационарные", "Грили и раклетницы",
        "Измельчители", "Кофеварки капельные", "Кофемашины автоматические", "Кофемашины капсульные", "Кофемолки",
        "Кухонные комбайны", "Микроволновые печи", "Миксеры", "Мультиварки", "Мясорубки", "Соковыжималки", "Чайники",
        "Электрочайники",
        "Гладильные доски", "Гладильные системы", "Мешки-пылесборники", "Парогенераторы", "Пылесосы", "Утюги"
    ]

    config = Configuration()
    xls_files_directory = config.folders["xls_folder"]
    # xls_files_directory = "./misc"
    files = [os.path.join(xls_files_directory, file) for file in os.listdir(xls_files_directory) if file.endswith(".xls")]  #[:15]
    categories = [Category(name, i + 1) for i, name in enumerate(categories)]
    queue = queue.Queue()

    # with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor1:
    #     # Flushing to CSV
    #     # sql_thread = CSV_PUSH()
    #     executor1.submit(CSV_PUSH)

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        # Starting threads for file parsing
        results = [executor.submit(parse_dns_xls_local, file, categories, queue) for file in files]
        # concurrent.futures.wait(results, return_when=concurrent.futures.ALL_COMPLETED)

    concurrent.futures.wait(results, return_when=concurrent.futures.ALL_COMPLETED)
    queue.put("EXIT")
    CSV_PUSH()