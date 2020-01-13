import requests
import re


class DnsWebsite:

    def __init__(self):
        self.url = "http://dns-shop.ru"

        self.base_cities_url = "https://www.dns-shop.ru/ajax/region-nav-window/"
        self.base_city_guid_url = "https://www.dns-shop.ru/ajax/change-city/?city_guid="
        self.base_zip_url = "https://www.dns-shop.ru/files/price/"
        self.base_device_search_url = "https://www.dns-shop.ru/search/?q="

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
            "referer": self.url
        }
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

    def download_dns_zip(self, params):
        '''Скачивает zip-архив с сайта, сохраняет в указанную папку.
        params = list[ссылка на сайте, адрес сохранения]
        '''
        url, save_path = params
        r = requests.get(url, headers=self.headers)
        with open(save_path, "wb") as f:
            f.write(r.content)
        print(f"Downloaded  {url} to: {save_path}")
