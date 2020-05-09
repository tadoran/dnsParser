import requests
import pandas as pd
import hashlib
import pickle

class DnsWebsite:

    def __init__(self):
        self.url = "http://dns-shop.ru"
        self.base_zip_url = "https://www.dns-shop.ru/files/price/"
        self.base_device_search_url = "https://www.dns-shop.ru/search/?q="
        self.cities_json_url = "http://dns-shop.ru/files/pwa/city-info.json"
        self.shops_json_url = "http://dns-shop.ru/files/error-page/shops.json"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
            "referer": self.url
        }
        self._cities = None
        self._shops = None

    @property
    def cities(self):
        if self._cities is None:
            cities_json = requests.get(self.cities_json_url, headers=self.headers)
            self._cities = pd.DataFrame(cities_json.json()["cities"]).T
        return self._cities

    @property
    def shops(self):
        if self._shops is None:
            shops_json = requests.get(self.shops_json_url, headers=self.headers)
            shops_frame = pd.DataFrame()
            for city_hash, city_shops_types in shops_json.json().items():
                for shop_type in city_shops_types:
                    cur_city_frame = pd.DataFrame(city_shops_types[shop_type])
                    cur_city_frame["shop_type"] = shop_type
                    cur_city_frame["city_hash"] = city_hash

                    shops_frame = pd.concat([shops_frame, cur_city_frame])
            shops_frame["addr_md5"] = shops_frame.address.apply(self.make_md5)
            self._shops = shops_frame.reset_index(drop=True)
        return self._shops

    def price_zip_urls(self):
        return {self.url + x.replace("xls", "zip") for x in self.cities["priceUrl"]}

    def price_zip_save_paths(self, base_folder=".\\"):
        return {base_folder + x.replace("/files/price/", "").replace("xls", "zip") for x in self.cities["priceUrl"]}

    def download_list(self, base_folder=".\\"):
        print(self.price_zip_urls())
        print(self.price_zip_save_paths(base_folder))
        return list(zip(self.price_zip_urls(), self.price_zip_save_paths(base_folder)))

    def download_dns_zip(self, params):
        '''Скачивает zip-архив с сайта, сохраняет в указанную папку.
        params = list[ссылка на сайте, адрес сохранения]
        '''
        url, save_path = params
        r = requests.get(url, headers=self.headers, timeout=25)
        with open(save_path, "wb") as f:
            f.write(r.content)
        print(f"Downloaded  {url} to: {save_path}")

    def make_md5(self, txt):
        return hashlib.md5(str(txt).encode('utf-8')).hexdigest()

    def save_pickle(self):
        with open('site.pickle', 'wb') as f:
            pickle.dump(self.__dict__, f)

    def load_pickle(self):
        with open('site.pickle', 'rb') as f:
            tmp_dict = pickle.load(f)
            self.__dict__.clear()
            self.__dict__.update(tmp_dict)


if __name__ == "__main__":
    site = DnsWebsite()
    print(site.download_list("C:\\Users\\Gorelov\\Desktop\\DNS Parser\\pyZip\\"))
    print(len(site.shops))
    site.save_pickle()
