import requests
import os
import time
import concurrent.futures

from db import db
from files.files import file_exists, days_from_creation
from config.config import Configuration


def make_download_list(files_count_limit=0, notifications=True):
    cur_con = db.DatabaseConnection()
    cities = cur_con.get_all_cities()
    config = Configuration()

    zip_files_directory = config.folders["zip_folder"]
    base_url = config.web["base_zip_url"]

    download_list = []
    counter = 0
    for city in cities:
        if 0 < files_count_limit <= counter: break

        if city["city_archive"]:
            city_url = base_url + city["city_archive"]
            save_path = os.path.join(zip_files_directory, city["city_archive"])
            if file_exists(save_path):
                # print(f"{save_path} is present")
                file_existence_days = days_from_creation(save_path)
                if file_existence_days == 0:
                    # if notifications:
                    #    print(f"{save_path} was downloaded recently. It will be skipped.")
                    continue
                else:
                    pass
                    # if notifications:
                    #    print(f"{save_path} was downloaded {file_existence_days} days ago. Proceeding with download.")

            # print(f"Downloading  {city_url} to: {save_path}")
            # download_dns_zip(city_url, save_path)
        download_list.append([city_url, save_path])
        counter += 1
    # print("Done.")
    return download_list

def download_dns_zip(params):
    url, save_path = params
    headers = ({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
                "referer": "https: // www.dns-shop.ru"
                })
    r = requests.get(url, headers=headers)
    with open(save_path, "wb") as f:
        f.write(r.content)
    print(f"Downloaded  {url} to: {save_path}")


if __name__ == "__main__":
    t1 = time.perf_counter()

    download_list = make_download_list(files_count_limit=0, notifications=False)
    print(download_list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(download_dns_zip, download_list)

    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')