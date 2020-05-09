from config.config import Configuration
import os
import csv
import time
import concurrent.futures
import zipfile

from datetime import date
import pandas as pd

from web.dns_website_obj import DnsWebsite
from files.xls_obj import Dns_parse_file


def comma_str(lst):
    ret_str = ",".join([str(x) for x in lst])
    return ret_str.replace(".0", "")


class Category:
    def __init__(self, name, id):
        self.nameGroup = name
        self.Id = id

    def __str__(self):
        return f"({self.Id}) {self.nameGroup}"


def parse_dns_xls(file_path, shops_from_site, categories):
    category_names = [obj.nameGroup for obj in categories]
    try:
        with Dns_parse_file(filename=file_path, categories=category_names) as dns_file:
            parsed_file = dns_file.parse()
            file_devices = parsed_file.devices
            file_shops = parsed_file.shops
            availability_by_shops = parsed_file.availability_by_shops
    except:
        filename_short = file_path.split("\\")[-1]
        print(f"{filename_short} (parse_dns_xls):Error reading file")
        return {}

    shop_ids = (pd.DataFrame
                .from_dict(file_shops, orient='index')
                .rename_axis(["ShopMD5"])
                .reset_index()
                .merge(
        left_on="ShopMD5",
        right_on="addr_md5",
        right=shops_from_site,
        how="left"
    )
                .set_index('ShopMD5')
                )

    try:
        parse_results_dic = (pd.DataFrame
                                 .from_dict(availability_by_shops, orient='index')
                                 .rename_axis(["Article", "Code"])
                                 .reset_index().merge(shop_ids, left_on="Code", right_on="Code")
                                 .set_index(["Article", "shopNum"])
                                 .loc[:, ["Price", "ProzaPass", "City", "addr_md5"]]
                                 ).to_dict(orient="index")
        # availability_dic.update(parse_results_dic)
        # models_dic.update(file_devices)


        str_output = f"\nOK - {file_path}"
        print(str_output, end="")


        return {"availability": parse_results_dic,
                "models": file_devices}
    except:
        print(f"{parsed_file.filename_short} (parse_dns_xls):Error in parse_results_dic, file")
        return {}

# def main_routine():
if __name__ == "__main__":
    t1 = time.perf_counter()
    categories = [
        "Варочные панели газовые", "Варочные панели электрические", "Встраиваемые посудомоечные машины",
        "Духовые шкафы электрические", "Посудомоечные машины", "Стиральные машины", "Холодильники", "Вытяжки",
        "Блендеры погружные", "Блендеры стационарные", "Грили и раклетницы",
        "Измельчители", "Кофеварки капельные",
        "Кофемашины автоматические", "Кофемашины капсульные", "Кофемолки",
        "Кухонные комбайны", "Микроволновые печи", "Миксеры", "Мультиварки", "Мясорубки", "Соковыжималки", "Чайники",
        "Электрочайники",
        "Гладильные доски", "Гладильные системы", "Мешки-пылесборники", "Парогенераторы", "Пылесосы", "Утюги",
        "Встраиваемые стиральные машины"
    ]

    config = Configuration()
    xls_files_directory = config.folders["xls_folder"]
    dns = DnsWebsite()
    dns.load_pickle()

    shops_df = dns.shops.reset_index(drop=True).reset_index()
    shops_df.drop_duplicates(inplace=True, subset="addr_md5")
    shop_ids_df = shops_df[["index", "addr_md5"]]
    shop_ids_df.columns = ["shopNum", "addr_md5"]

    dns.save_pickle()

    files = [os.path.join(xls_files_directory, file) for file in os.listdir(xls_files_directory) if
             file.endswith(".xls")] #[:15]
    # ]
    categories = [Category(name, i + 1) for i, name in enumerate(categories)]

    models = {}
    availability = {}

    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Starting threads for file parsing
        results = [executor.submit(parse_dns_xls, file, shop_ids_df, categories) for file in files]
    concurrent.futures.wait(results, return_when=concurrent.futures.ALL_COMPLETED)
    for file_result in results:
        for dict_name, dict_values in file_result.result().items():
            locals()[dict_name].update(dict_values)

    if models == {}:
        print(f"Models were not set correctly")
    models_df = pd.DataFrame.from_dict(models, orient="index").loc[:, ["Descr", "Category"]]
    availability_df = pd.DataFrame.from_dict(availability, orient="index").rename_axis(["Артикул", "Магазин"])
    data_df = availability_df.rename_axis(["Article", "Shop"]).reset_index().groupby(["City", "Article"]) \
        .agg({"Price": min, "ProzaPass": min, "Shop": [comma_str, "count"]}).reset_index()

    data_df.columns = ["Файл", "Артикул", "Цена", "ProzaPass", "Магазины", "Кол-во магазинов"]

    parsing_date = date.today()
    date_str = parsing_date.strftime("%d.%m.%Y")

    base_path = "./output/"
    output_zip_name = f"output {date_str}.zip"
    models_file = f"models {date_str}.csv"
    shops_file = f"shops {date_str}.csv"
    availability_file = f"availability {date_str}.csv"
    data_file = f"data {date_str}.csv"

    print()
    # print("Writing Shops")
    shops_df["date"] = date_str
    shops_df_final = shops_df \
        .set_index("addr_md5") \
        .astype(str) \
        .merge(
        how="left",
        right=availability_df.loc[:, ["addr_md5", "City"]],
        left_index=True,
        right_index=False,
        right_on="addr_md5") \
        .drop_duplicates(keep="first") \
        .set_index("addr_md5").astype(str)

    # with open(shops_file, 'w', newline='', encoding="utf-8") as f:
    #     f.write('\ufeff' + shops_df_final.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))

    # print("Writing Models")
    models_df["Дата"] = date_str
    # with open(models_file, 'w', newline='', encoding="utf-8") as f:
    #     f.write('\ufeff' + models_df.rename_axis('Артикул').to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))

    # print("Writing Data")
    data_df["Дата"] = date_str
    data_df = data_df[["Дата", "Артикул", "Файл", "Магазины", "Цена", "ProzaPass", "Кол-во магазинов"]]
    # with open(data_file, 'w', newline='', encoding="utf-8") as f:
    #     f.write('\ufeff' + data_df.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",", index=False))

    # print("Writing Availability")
    availability_df["Дата"] = date_str
    # with open(availability_file, 'w', newline='', encoding="utf-8") as f:
    #     f.write('\ufeff' + availability_df.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))

    with zipfile.ZipFile(os.path.join(base_path, output_zip_name), 'w', zipfile.ZIP_LZMA) as z:
        print("Writing Shops")
        #     f.write('\ufeff' + shops_df_final.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))
        # z.write()
        z.writestr(shops_file,
                   '\ufeff' + shops_df_final.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))

        print("Writing Models")
        #     f.write('\ufeff' + models_df.rename_axis('Артикул').to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))
        z.writestr(models_file,
                   '\ufeff' + models_df.rename_axis('Артикул').to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL,
                                                                      decimal=","))

        print("Writing Data")
        #     f.write('\ufeff' + data_df.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",", index=False))
        z.writestr(data_file,
                   '\ufeff' + data_df.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=",", index=False))

        print("Writing Availability")
        #     f.write('\ufeff' + availability_df.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))
        z.writestr(availability_file,
                   '\ufeff' + availability_df.to_csv(sep=";", quotechar='"', quoting=csv.QUOTE_ALL, decimal=","))

    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')

# if __name__ == "__main__":
#     main_routine()
