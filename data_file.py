import pandas as pd

a = r'C:\Users\Gorelov\Documents\DNS PyParsing\output\\'
date_base = " 08-01-2020"

availability_filename = a + "availability" + date_base + ".csv"
availability_headers = ["Article", "Price", "ProzaPass", "Shop", "Date"]
availability = pd.read_csv(
    availability_fileneme,
    sep=";",
    names=availability_headers,
    parse_dates=True
)
# availability.head()
# availability.info()

shops_filename = a + "shops" + date_base + ".csv"
shops_headers = ["shop_id", "shop_name", "shop_phone", "shop_worktime", "shop_address", "shop_city", "shop_num"]
shops = pd.read_csv(shops_filename, sep=";", names=shops_headers, parse_dates=True, encoding="utf-8")
shops.head()


def comma_str(lst):
    return ",".join([str(x) for x in lst])


data = (
        availability
            .merge(right=shops, how="left", left_on="Shop", right_on="shop_id")
            .groupby(["shop_city", "Article"])
            .agg(
                {
                    "Price": min,
                    "ProzaPass": min,
                    "shop_num": [comma_str, "count"],
                    "Date": min
                }
            )
        .reset_index()
        )

data.columns = ["city", "article", "price", "ProzaPass", "shops", "shops_count", "date"]
data.date = pd.to_datetime(data.date)

data.to_csv(
    a + "data" + date_base + ".csv",
    sep=";",
    header=True,
    index=False,
    quoting=1,  # QUOTE_ALL
    date_format="%d.%m.%Y"  # 08.01.2020
)
