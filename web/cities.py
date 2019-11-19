import requests
import re

from config.config import Configuration


def get_cities_and_hashes():
    ''' Возвращает список всех городов и их hash с сайта ДНС
    Возвращает dict("Город":hash)
    '''
    config = Configuration("config.ini")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"}
    response = requests.get(config.web["base_cities_url"], headers=headers)

    pattern = r'<a[\s\S]+?data-city-id="+([\s\S]+?)\"+[\s\S]+?<span>([\s\S]+?)<\/span>[\s\S]+?<\/a>'
    results = re.findall(pattern, response.json()["html"])

    res = {cityName: hashStr for (hashStr, cityName) in results}
    return res


if __name__ == "__main__":
    for x, y in get_cities_and_hashes().items():
        print(x, y)
