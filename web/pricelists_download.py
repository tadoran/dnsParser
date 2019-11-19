import requests
import os
import time
import concurrent.futures

from db import db
from files.files import file_exists, days_from_creation, clear_folder
from files.zip import unzip_file
from config.config import Configuration


def make_download_list(files_count_limit=0, notifications=True):
    """Возвращает список ссылок на архивы прайс-листов на сайте.
    Возвращаются ссылки на архивы, которые отсутствуют в  скаченных, или скаченные не сегодня.
    files_count_limit - ограничение на кол-во возвращаемых ссылок.
    Возвращается List([ссылка на сайте, адрес сохранения] )
    """
    cur_con = db.DatabaseConnection()
    cities = cur_con.get_all_cities()
    # config = Configuration()

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
                    continue
                else:
                    pass

        download_list.append([city_url, save_path])
        counter += 1
    return download_list


def download_dns_zip(params):
    '''Скачивает zip-архив с сайта, сохраняет в указанную папку.
    params = list[ссылка на сайте, адрес сохранения]
    '''
    url, save_path = params
    headers = ({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
        "referer": "https: // www.dns-shop.ru"
    })
    r = requests.get(url, headers=headers)
    with open(save_path, "wb") as f:
        f.write(r.content)
    print(f"Downloaded  {url} to: {save_path}")


def unzip_dns_xls(params):
    '''Распаковывает zip-архив в указаную директорию.
    params = List[адрес zip-архива, директория сохранения]
    '''
    zip_path, save_path = params
    unzip_file(zip_path, save_path)


if __name__ == "__main__":
    t1 = time.perf_counter()
    config = Configuration()
    zip_files_directory = config.folders["zip_folder"]
    xls_files_directory = config.folders["xls_folder"]

    clear_folder(zip_files_directory)
    clear_folder(xls_files_directory)

    download_list = make_download_list(files_count_limit=0, notifications=False)
    print(download_list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        executor.map(download_dns_zip, download_list)

    print(zip_files_directory, xls_files_directory)
    files = [zip_files_directory + "\\" + file for file in os.listdir(zip_files_directory) if file.endswith(".zip")]
    print(files)
    downloaded_zips = [(file, xls_files_directory) for file in files]
    print(downloaded_zips)

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        executor.map(unzip_dns_xls, downloaded_zips)

    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')
