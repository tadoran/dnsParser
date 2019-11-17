import xlrd
import re
import string
import hashlib


def parse_xl_rangeRef(rangeRef):
    ascii_uppercase = " " + string.ascii_uppercase
    pattern = "'([\s\S]+?)'!(\w+?)(\d+)"
    r = re.findall(pattern, rangeRef)
    # print()
    matched_list = list(r[0])
    matched_list[0] = r[0][0]
    matched_list[1] = int(r[0][2])
    matched_list[2] = ascii_uppercase.find(r[0][1])
    return matched_list


def parse_dns_shop_entry(shop_entry):
    pattern = "(М\d+)\s?—\s?([\s\S]+?), тел.([\s\S]+?)\s*\.\s*Режим работы:\s?([\s\S]+?)\s*\.\s*Адрес:\s?([\s\S]+)"
    r = re.findall(pattern, shop_entry)
    hash_object = hashlib.md5(str(r[0][4]).encode('utf-8'))
    return list(r[0]) + [hash_object.hexdigest()]


def parse_dns_xls(file_path):
    print(f'Call to parse {file_path}')

    # TODO: брать категории для парсинга из базы
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

    with xlrd.open_workbook(filename=file_path, on_demand=True) as book:
        # Ищемм ссылки на диапазоны в листе содержания
        title_sh = book.sheet_by_index(0)

        # Записываем все ссылки. Если один текст встречается несколько раз - оставляем последнюю запись.
        sh_links = [[title_sh.cell(link.frowx, link.fcolx).value.strip(), link.textmark] for link in
                    title_sh.hyperlink_list]
        sh_links_filtered = {text: [link, parse_xl_rangeRef(link)] for text, link in sh_links if text in categories}

        # Конвертируем ссылки Excel на [Sheet, Row, Column], записываем [[SheetName, Row]] (колонка всегда 1)
        sheet_links_to_parse = \
            [[sheet_info[1][0], sheet_info[1][1], text] for text, sheet_info in sh_links_filtered.items()]

        # Листы для разбора (присутствуют в sh_links_filtered)
        sheets_to_parse = {sheet_info[1][0]: list() for sheet_info in sh_links_filtered.values()}
        for sh_name, sh_rows, category in sheet_links_to_parse:
            # Записываем в словарь вида {SheetName:[sheet_row1,sheet_row2,...]}
            sheets_to_parse[sh_name].append((sh_rows, category))

        # Словарь с магазинами. Ключ - MDS(Адрес) : [Код, Название, Телефон, Время работы, Адрес]
        city_shops = dict()
        shops_retrieved = False  # Парсим магазины только 1 раз, они на всех листах одинаковые
        # Словарь с наличием. Ключ - int(Артикул)
        data_retrieved = dict()

        for sheet_name, sheet_rows in sheets_to_parse.items():
            # sheet_rows = sorted(sheet_rows)  # Сортируем ссылки на листе по возростанию, идем сверху вниз
            sheet = book.sheet_by_name(sheet_name)  # Подгружаем лист

            # Парсинг магазинов
            if not shops_retrieved:
                row = 0
                entry = ""
                while entry != "Код" or row < sheet.nrows - 1:
                    row += 1
                    entry = str(sheet.row_values(row)[0])
                    if entry == "Код":
                        shops_retrieved = True
                        break
                    parsed_string = parse_dns_shop_entry(entry)
                    # city_shops[parsed_string[5]] = parsed_string[:5]
                    city_shops[parsed_string[5]] = ({
                        # Код, Название, Телефон, Время работы, Адрес
                        "Code":     parsed_string[0],
                        "Name":     parsed_string[1],
                        "Phone":    parsed_string[2].strip(),
                        "WorkTime": parsed_string[3],
                        "Address":  parsed_string[4]
                    })

            # Парсинг наличия
            for row, category in sheet_rows:
                counter = 0
                article = ""
                # Спускаемся от первой ячейки ссылки вниз пока не встретим "Код" или не дойдем до конца листа
                while article != "Код" or row < sheet.nrows - 1:
                    article, article_name, *availability, price, prozaPass = sheet.row_values(row)
                    if article == "Код":
                        break

                    available_in = list(filter(bool, availability))
                    data_retrieved[int(article)] = ({
                        # "Descr": bytes(article_name,'cp1251').decode('cp1251', 'ignore'),
                        "Descr": article_name,
                        "Price": int(price),
                        "ProzaPass": int(prozaPass),
                        "AvailableIn": available_in,
                        "AvailableCount": len(available_in),
                        "Category": category
                    })

                    if row < sheet.nrows - 1:
                        row += 1
                    else:
                        break

            # Выгружаем лист из памяти для экономии ресурсов
            book.unload_sheet(sheet_name)
    return [city_shops, data_retrieved]


if __name__ == "__main__":
    filename = 'C:\\Users\\Gorelov\\Desktop\\DNS Parser\\pyXls\\price-spb.xls'
    # filename = 'C:\\Users\\Gorelov\\Desktop\\DNS Parser\\pyXls\\price-pereaslavka.xls'
    parse_results = parse_dns_xls(filename)
    print("Shop details")
    for shop_hash, shop_info in parse_results[0].items():
        print(shop_hash)
        for key, value in shop_info.items():
            print(f"{key}: {value}")
        print()

    print()
    print("Articles details")
    for article, article_details in  parse_results[1].items():
        print(article)
        for key, value in article_details.items():
            print(f"{key}: {value}")
        print()

    print()
    print(f"Total shops: {len(parse_results[0])}")
    print(f"Total articles:{len(parse_results[1])}")
