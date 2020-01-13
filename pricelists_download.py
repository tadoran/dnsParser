import os
import time
import zipfile
import concurrent.futures

from db import db
from files.files import file_exists, days_from_creation, clear_folder
from config.config import Configuration
from web.dns_website_obj import DnsWebsite


def make_download_list(save_dir, cities, base_zip_url, files_count_limit=0):
    """Возвращает список ссылок на архивы прайс-листов на сайте.
    Возвращаются ссылки на архивы, которые отсутствуют в  скаченных, или скаченные не сегодня.
    files_count_limit - ограничение на кол-во возвращаемых ссылок.
    Возвращается List([ссылка на сайте, адрес сохранения] )
    """
    download_list = []
    for counter, city in enumerate(cities):
        if 0 < files_count_limit <= counter: break

        if city["city_archive"]:
            city_url = base_zip_url + city["city_archive"]
            save_path = os.path.join(save_dir, city["city_archive"])
            if file_exists(save_path):
                file_existence_days = days_from_creation(save_path)
                if file_existence_days == 0:
                    continue
                else:
                    pass
        download_list.append([city_url, save_path])
        counter += 1
    return download_list


def unzip_dns_xls(params):
    '''Распаковывает zip-архив в указаную директорию.
    params = List[адрес zip-архива, директория сохранения]
    '''
    zip_path, save_path = params
    print(f"Unzipping {zip_path} to {save_path}")
    with zipfile.ZipFile(zip_path) as zip_file:
        for names in zip_file.namelist():
            zip_file.extract(names, save_path)


if __name__ == "__main__":
    t1 = time.perf_counter()

    website = DnsWebsite()
    connection = db.DatabaseConnection()
    config = Configuration()

    zip_files_directory = config.folders["zip_folder"]
    xls_files_directory = config.folders["xls_folder"]

    # Все города из БД
    all_cities = cityDict = [x["city_name"] for x in connection.get_all_cities()]
    # Скачивание GUID городов с сайта. Возвращаем только те, которых нет в БД
    new_cities = [(name, city_hash) for name, city_hash in website.get_cities_and_hashes().items() if name not in all_cities]

    # Сохраняем новые города в БД
    print(connection.put_new_cities_to_db(new_cities))

    # Список городовв БД, у которых есть GUID, но нет названия файла
    cityDict = {x['city_name']: x for x in connection.get_all_cities_wo_filename()}
    # Для каждого такого города - переходим на его страницу, вытаскиваем название
    for city in cityDict.keys():
        city_dns_name = website.get_city_info(cityDict[city]["city_hash"])
        # Если для города удалось найти имя
        if city_dns_name:
            city_dns_xls_name = "price-" + city_dns_name + ".xls"
            city_dns_zip_name = "price-" + city_dns_name + ".zip"
            # Выводим эти города
            print(city, cityDict[city]["cities_id"], cityDict[city]["city_hash"], city_dns_name, city_dns_xls_name,
                  city_dns_zip_name)
            # Записываем имена городов в БД
            print(connection.put_dns_names_to_db(city_dns_name, city_dns_xls_name, city_dns_zip_name,
                                                 cityDict[city]["cities_id"]))
        #  Если не удалось найти имя - выведем None
        else:
            print(city, cityDict[city]["cities_id"], None)

    cities = connection.get_all_cities()

    # Очистка папок
    clear_folder(zip_files_directory)
    clear_folder(xls_files_directory)

    # Формируем список файлов на загрузку
    download_list = make_download_list(
        files_count_limit=0,
        save_dir=zip_files_directory,
        cities=cities,
        base_zip_url=website.base_zip_url
    )

    # Скачивание файлов в x потоков
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        executor.map(website.download_dns_zip, download_list)

    # Все скаченные zip-ахривы
    files = [zip_files_directory + "\\" + file for file in os.listdir(zip_files_directory) if file.endswith(".zip")]
    # [(Файл, папка для распаковки)]
    downloaded_zips = [(file, xls_files_directory) for file in files]

    # Раззиповка, в x потоков
    with concurrent.futures.ProcessPoolExecutor(max_workers=15) as executor:
        executor.map(unzip_dns_xls, downloaded_zips)

    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')
