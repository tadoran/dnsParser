import requests
base_url = "https://www.dns-shop.ru/search/"
search_text = "1119558 Утюг Bosch TDA702821А синий"
headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
            "referer": base_url
        }
results_dic = {}
arr = ([
        "1154073 Стиральная машина LG FH2H3ND1",
        "1162371 Стиральная машина LG FH2H3WDS2",
        "1037518 Стиральная машина Samsung WF8590NLW8DYLP",
        "1133539 Стиральная машина Samsung WW65J42E0HWDLP",
        "8121105 Утюг Aresa AR-3115 чёрный",
        "1904452 Утюг Bosch TDA2365 серый",
        "1157010 Утюг Bosch TDA30EASY серый",
        "8146998 Вытяжка полновстраиваемая BEST BHG71220GA серебристый",
        "8147000 Вытяжка полновстраиваемая BEST BHG76550XA серебристый",
        "8147001 Вытяжка полновстраиваемая BEST BHG76551XA серебристый",
        "8124044 Вытяжка полновстраиваемая BEST GARDA XS 52 P580 XS 52 серебристый",
        "1126454 Вытяжка полновстраиваемая Cata TF 5060 WH/D белый",
        "Холодильники",
        "Видеокарта",
        "Молоко"
        ])
for search_text in arr:
    r = requests.get(base_url, params={"q":search_text}, headers=headers, allow_redirects = False)
    if r.status_code == 302:
        results_dic[search_text] =  r.headers.get("Location", None)
        # print(r.headers.get("Location",None))
        # r = requests.get(url, headers=headers)
    else:
        results_dic[search_text] =  r.headers.get("Location", None)

print(results_dic)