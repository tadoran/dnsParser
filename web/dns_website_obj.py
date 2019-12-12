import requests
import re
import json


class Dns_website:
    def __init__(self):
        self.headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
                        "referer": "https: // www.dns-shop.ru"
                        }
        self.url = "http://dns-shop.ru"
        self.base_cities_url = "https://www.dns-shop.ru/ajax/region-nav-window/"
        self.base_city_guid_url = "https://www.dns-shop.ru/ajax/change-city/?city_guid="
        self.cities = {}


    def get_cities_and_hashes(self):
        """ Возвращает список всех городов и их hash с сайта ДНС
        Возвращает dict("Город":hash) """
        response = requests.get(self.base_cities_url, headers=self.headers)
        pattern = r'<a[\s\S]+?data-city-id="+([\s\S]+?)\"+[\s\S]+?<span>([\s\S]+?)<\/span>[\s\S]+?<\/a>'
        results = re.findall(pattern, response.json()["html"])
        self.cities = {cityName: hashStr for (hashStr, cityName) in results}
        return self.cities

    def get_city_info(self, city_hash):
        """ Возвращает cookie с названием города с сайта DNS (cookie city = moscow игнорируется) """
        response = requests.get(self.base_city_guid_url + city_hash, headers=self.headers)
        for cookie in response.cookies:
            if (cookie.name == "city_path") & (cookie.value != "moscow"):
                return cookie.value
        return None