from config.config import Configuration
import os
import concurrent.futures
import queue
import time

from datetime import date
# from files.xls import parse_dns_xls
from files.xls_obj import Dns_parse_file
from db.models import Device, Shop, Availability
from db.define_sql import get_session, get_categories


def parse_dns_xls_local(*args):
    # Name of parsed file
    filename = args[0]
    # Categories for import
    categories = args[1]
    queue = args[2]

    # print(f"Request for parsing {filename}")
    print("Req", end="   ")
    # SQL imports
    category_names = [obj.nameGroup for obj in categories]
    category_dict = {obj.nameGroup: obj.Id for obj in categories}

    # File imports
    parsed_file = Dns_parse_file(filename, category_names).parse()
    file_devices = parsed_file.devices
    file_shops = parsed_file.shops
    parse_results = parsed_file.availability_by_shops

    queue.put([file_devices, file_shops, parse_results, category_dict])


def sql_async(endless=True):
    global queue
    print("SQL_ASYNC iteration starts")
    # SQL Session
    session = get_session()

    # Main loop
    while True:
        shops = {}
        models = {}
        availability = {}
        category_dict = {}
        # Concatenate all present results together
        counter = 0
        while not queue.empty() and counter <= 10:
            # print(f"There are {len(list(queue.queue))} items in queue now.")
            qur_queue_result = queue.get()
            if qur_queue_result == "EXIT":
                print("SQL_ASYNC: I see EXIT element. Closing soon.")
                if counter == 0:
                    return
                else:
                    endless = False
                    continue
            cur_models = qur_queue_result[0]
            cur_shops = qur_queue_result[1]
            cur_availability = qur_queue_result[2]
            cur_category_dict = qur_queue_result[3]

            models.update(cur_models)
            shops.update(cur_shops)
            availability.update(cur_availability)
            category_dict.update(cur_category_dict)
            counter += 1

        if counter == 0:
            time.sleep(1)
        else:
            print(f"SQL_ASYNC collected {counter} file results. Processing.")

            ### DEVICES
            # SQL query - table `devices` - get all
            sql_existing_devices = session.query(Device).filter(
                Device.device_article.in_(list(models.keys()))).all()
            sql_existing_devices_articles = [obj.device_article for obj in sql_existing_devices]

            # File devices not in SQL
            new_devices = {key: el for key, el in models.items() if key not in sql_existing_devices_articles}
            if len(new_devices) > 0:
                # If there are new devices in file - upload to SQL
                for article, details in new_devices.items():
                    new_device = Device(
                        device_article=article,
                        group_ID=category_dict.get(details["Category"], None),
                        device_name_dns=details["Descr"]
                    )
                    session.add(new_device)
                # print(f"Adding {len(new_devices)} new devices to DB")
                session.commit()

            ### SHOPS
            # SQL query - table `shops` - get all
            sql_existing_shops = session.query(Shop).filter(Shop.MD5.in_(list(shops.keys()))).all()
            # print(f"sql_existing_shops: {sql_existing_shops}")
            sql_existing_shops_ids = [obj.MD5 for obj in sql_existing_shops]
            # print(f"sql_existing_shops_ids: {sql_existing_shops_ids}")
            # File shops not in SQL
            new_shops = {key: el for key, el in shops.items() if key not in sql_existing_shops_ids}
            # print(f"new_shops: {new_shops}")
            if len(new_shops) > 0:
                # If there are new shops in file - upload to SQL
                for MD5_hash, details in new_shops.items():
                    new_shop = Shop(
                        MD5=MD5_hash,
                        Name=details["Name"],
                        Phone=details["Phone"],
                        worktime=details["WorkTime"],
                        Address=details["Address"]
                    )
                    session.add(new_shop)
                # print(f"Adding {len(new_shops)} new shops to DB")
                session.commit()

            # print(parse_results)
            # print(file_shops)
            sql_existing_shops = session.query(Shop).filter(Shop.MD5.in_(list(shops.keys()))).all()
            sql_existing_shops_ids = {obj.MD5: obj.id for obj in sql_existing_shops}
            file_shopcode_shopid = {value["Code"]: sql_existing_shops_ids[key] for key, value in shops.items()}
            # print(file_shopcode_shopid)

            # Appending SQL shop.ID to parse_results keys
            parse_results_w_shop_id = dict()
            for key, value in availability.items():
                parse_results_w_shop_id[(key[0], file_shopcode_shopid[key[1]])] = value

            ### AVAILABILITY
            # TODO: make date different
            parsing_date = date.today()
            # SQL query - table `availability` - get shop-article pairs on current date
            sql_existing_shop_article = \
                session.query(Availability.device_article, Availability.shop) \
                    .filter(Availability.date == parsing_date) \
                    .filter(Availability.device_article.in_(list(models.keys()))) \
                    .filter(Availability.shop.in_(file_shopcode_shopid.values())).all()
            # tuples for values in SELECTion
            sql_shop_article_tuples = [(obj.device_article, obj.shop) for obj in sql_existing_shop_article]

            # INSERT INTO `availability` if (article, shop_id) NOT IN sql_shop_article_tuples
            entries_to_add = 0
            entries_to_skip = 0
            for key, value in parse_results_w_shop_id.items():
                if key not in sql_shop_article_tuples:
                    entry = Availability(
                        device_article=key[0],
                        price=value["Price"],
                        prozaPass=value["ProzaPass"],
                        shop=key[1],
                        date=parsing_date
                    )
                    session.add(entry)
                    entries_to_add += 1
                else:
                    entries_to_skip += 1

            # print(f"Adding {entries_to_add} new availability entries to DB (skipping {entries_to_skip})")

            session.commit()
            print(f"SQL_ASYNC:\nAdding new: devices: {len(new_devices)}, shops: {len(new_shops)}, " +
                  f" available: {entries_to_add} ({entries_to_skip} skipped).\n")
        if endless == False:
            print("SQL_ASYNC: Bye-bye!")
            break
        print("SQL_ASYNC going to next lap.")


config = Configuration()
xls_files_directory = config.folders["xls_folder"]
# xls_files_directory = "./misc"
files = [os.path.join(xls_files_directory, file) for file in os.listdir(xls_files_directory) if file.endswith(".xls")]

print(files)
categories = get_categories()
queue = queue.Queue()
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    sql_thread = executor.submit(sql_async)
    print(f"Asserting that at least 1 result is parsed. Starting pushing to SQL")
    # Starting threads for file parsing
    results = [executor.submit(parse_dns_xls_local, file, categories, queue) for file in files]

    # On completion of file thread - push results to queue
    # for f in concurrent.futures.as_completed(results, timeout=120):
    #     print("Got some results. Putting them to queue.")
    #     queue.put(f)

    # Thread to push data to DB
    concurrent.futures.wait(results, return_when=concurrent.futures.FIRST_COMPLETED)
# with concurrent.futures.ProcessPoolExecutor() as executor:

    concurrent.futures.wait(results, return_when=concurrent.futures.ALL_COMPLETED)
    print(f"Asserting that all threads are completed. Closing connection to SQL")
    queue.put("EXIT")
    # After all file threads are completed - kill SQL thread
    # print("All threads are done, killing SQL ")
    # sql_thread.cancel()
    # # If something is left in queue - call it once again without endless loop
    # if not queue.empty():
    #     sql_async(queue, False)
