import requests
import re
from config.config import Configuration
config = Configuration()


def get_city_info(city_hash):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
Chrome/77.0.3865.120 Safari/537.36"}
    url = config.web["base_city_guid_url"] + city_hash

    response = requests.get(url, headers=headers)
    city_path = None

    for cookie in response.cookies:
        if (cookie.name == "city_path") & (cookie.value != "moscow"):
            city_path = cookie.value
    return city_path


if __name__ == "__main__":
    moscow_hash = "30b7c1f3-03fb-11dc-95ee-00151716f9f5"
    aksay_hash = "ab170763-714b-11e9-a208-00155df1b805"
    print(get_city_info(aksay_hash))
