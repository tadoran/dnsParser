from xls_obj import Dns_parse_file

if __name__ == "__main__":

    categories = [
        "Варочные панели газовые", "Варочные панели электрические", "Встраиваемые посудомоечные машины",
        "Духовые шкафы электрические",
        "Посудомоечные машины", "Стиральные машины", "Холодильники", "Вытяжки", "Блендеры погружные",
        "Блендеры стационарные", "Грили и раклетницы",
        "Измельчители", "Кофеварки капельные", "Кофемашины автоматические", "Кофемашины капсульные", "Кофемолки",
        "Кухонные комбайны",
        "Микроволновые печи", "Миксеры", "Мультиварки", "Мясорубки", "Соковыжималки", "Чайники", "Электрочайники",
        "Гладильные доски",
        "Гладильные системы", "Мешки-пылесборники", "Парогенераторы", "Пылесосы", "Утюги"
    ]

    with Dns_parse_file("./misc/price-abakan.xls") as parse_file:
        parse_file.categories = categories
        parse_file.parse()
        # print(f"Total shops: {len(parse_file.shops)}, Total devices: {len(parse_file.devices)}")
        # print([details["Name"] for details in parse_file.shops.values()])

        # prices = [[details["Category"], details["Descr"], details["Price"], details["AvailableIn"]] for details in parse_file.availability.values()]
        db_entries = parse_file.availability_by_shops
        for entry in db_entries.items():
            print(entry)
        print(f"Entries for DB: {len(db_entries)}")
        # print(parse_file.availability)
        

#    parse_file = Dns_parse_file("./misc/price-abakan.xls")
#    parse_file.categories = categories
#    parse_file.parse()
#    print(f"Total shops: {len(parse_file.get_shops())}, Total devices: {len(parse_file.get_devices())}")