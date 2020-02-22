import os
import time
import zipfile
import concurrent.futures

from files.files import file_exists, days_from_creation, clear_folder
from config.config import Configuration
from web.dns_website_obj import DnsWebsite


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
    config = Configuration()

    zip_files_directory = config.folders["zip_folder"] + "\\"
    xls_files_directory = config.folders["xls_folder"] + "\\"

    # Очистка папок
    clear_folder(zip_files_directory)
    clear_folder(xls_files_directory)

    # Формируем список файлов на загрузку
    download_list = website.download_list(zip_files_directory)

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
