import sys
import os
from datetime import datetime, timedelta


def file_exists(filepath):
    """Определяет, существует ли файл
    Возвращает True / False
    """
    return os.path.exists(filepath)


def file_modified(filepath, return_as_date=False):
    """ Возвращает время модификации и создания файла
    Возвращает dict("Modified","Created")
    return_as_date = False - возвращает время в секундах
    return_as_date = True - возвращает datetime
    """
    if not file_exists(filepath):
        return None

    modified_time = os.path.getmtime(filepath)
    created_time = os.path.getctime(filepath)

    if return_as_date:
        return {"Modified": datetime.fromtimestamp(modified_time), "Created": datetime.fromtimestamp(created_time)}
    else:
        return {"Modified": modified_time, "Created": created_time}


def days_from_creation(filename):
    """Возвращает целое кол-во дней с момента модификации файла (int)
    """
    return (datetime.now() - file_modified(filename, True)["Modified"]).days


def clear_folder(folder_path):
    """ Удаляет все файлы в заданной папке
    """
    for the_file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    file_to_analyze = sys.argv[0]
    # file_to_analyze = r"C:\Users\Gorelov\Desktop\DNS Parser\pyZip\price-sochi.zip"

    print(file_to_analyze, file_exists(file_to_analyze), file_modified(file_to_analyze),
          days_from_creation(file_to_analyze))
    print(file_to_analyze, file_exists(file_to_analyze), file_modified(file_to_analyze, True),
          days_from_creation(file_to_analyze))
