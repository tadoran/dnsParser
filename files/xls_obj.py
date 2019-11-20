import xlrd
import re
import string
import hashlib

class Parse_file:
    def __init__(self, filename):
        self.filename = filename
        self.isParsed = False
        self._shops = {}
        self._articles = {}
        self._availability = {}
    
    def load_file(self):
        raise NotImplementedError
    
    @property
    def shops(self):
        return self._shops
    
    @shops.setter
    def shops(self, value):
        self._shops = value

    @property
    def devices(self):
        return self._articles
    
    @devices.setter
    def devices(self, value):
        self._articles = value

    @property
    def availability(self):
        return self._availability

    @availability.setter
    def availability(self,value):
        self._availability = value

    def parse(self):
        raise NotImplementedError


class Dns_parse_file(Parse_file):
    def __init__(self,filename, categories=[]):
        super().__init__(filename)
        self.categories = categories
        self.load_file()

    def load_file(self):
        # raise CompDocError("%s corruption: seen[%d] == %d" % (qname, s, self.seen[s]))
        # http://www.programmersought.com/article/676234398/
        # https://stackoverflow.com/questions/12705527/reading-excel-files-with-xlrd

        self.file = xlrd.open_workbook(filename=self.filename, on_demand=True)
    
    @property
    def availability_by_shops(self):
        by_shops = {}
        for key, device in self._availability.items():
            for shop in device.pop("AvailableIn",None):
                by_shops[(key, shop)] = device
        return by_shops

    def __enter__(self):
        self.load_file()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.release_resources()

    def parse(self):
        
        # Ищем ссылки на диапазоны в листе содержания
        title_sh = self.file.sheet_by_index(0)

        # Записываем все ссылки. Если один текст встречается несколько раз - оставляем последнюю запись.
        sh_links = [[title_sh.cell(link.frowx, link.fcolx).value.strip(), link.textmark] for link in
                    title_sh.hyperlink_list]
        sh_links_filtered = {text: [link, self.parse_xl_rangeRef(link)] for text, link in sh_links if text in self.categories}

        # Конвертируем ссылки Excel на [Sheet, Row, Column], записываем [[SheetName, Row]] (колонка всегда 1)
        sheet_links_to_parse = \
            [[sheet_info[1][0], sheet_info[1][1], text] for text, sheet_info in sh_links_filtered.items()]

        # Листы для разбора (присутствуют в sh_links_filtered)
        sheets_to_parse = {sheet_info[1][0]: list() for sheet_info in sh_links_filtered.values()}
        for sh_name, sh_rows, category in sheet_links_to_parse:
            # Записываем в словарь вида {SheetName:[sheet_row1,sheet_row2,...]}
            sheets_to_parse[sh_name].append((sh_rows, category))

        shops_retrieved = False  # Парсим магазины только 1 раз, они на всех листах одинаковые
        # Словарь с наличием. Ключ - int(Артикул)
        data_retrieved = dict()
        for sheet_name, sheet_rows in sheets_to_parse.items():
            sheet = self.file.sheet_by_name(sheet_name)  # Подгружаем лист
            if not shops_retrieved:
                self.parse_shops(sheet)
            data_retrieved.update(self.parse_availability(sheet,sheet_rows))
            self.file.unload_sheet(sheet_name)  # Выгружаем лист из памяти для экономии ресурсов

        self._articles = data_retrieved.keys()
        self._availability = data_retrieved
        self.isParsed = True
        return self

    def parse_availability(self, sheet,sheet_rows):
        # Парсинг наличия
        for row, category in sheet_rows:
            article = ""
            data_retrieved = {}
            # Спускаемся от первой ячейки ссылки вниз пока не встретим "Код" или не дойдем до конца листа
            while article != "Код" or row < sheet.nrows - 1:
                article, article_name, *availability, price, prozaPass = sheet.row_values(row)
                if article == "Код":
                    break

                available_in = list(filter(bool, availability))
                data_retrieved[int(article)] = ({
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
        
        return data_retrieved

    def parse_shops(self, sheet):
        # Парсинг магазинов
        row = 0
        entry = ""
        city_shops = {}
        while entry != "Код" or row < sheet.nrows - 1:
            row += 1
            entry = str(sheet.row_values(row)[0])
            if entry == "Код":
                break
            parsed_string = self.parse_dns_shop_entry(entry)
            city_shops[parsed_string[5]] = ({
                # Код, Название, Телефон, Время работы, Адрес
                "Code":     parsed_string[0],
                "Name":     parsed_string[1],
                "Phone":    parsed_string[2].strip(),
                "WorkTime": parsed_string[3],
                "Address":  parsed_string[4]
            })
        self._shops = city_shops

    @staticmethod
    def parse_xl_rangeRef(rangeRef):
        """Парсинг ссылки Excel вида 'Sheet1'!B3
        Возвращает list(sheet,row, column)
        """
        ascii_uppercase = " " + string.ascii_uppercase
        pattern = r"'([\s\S]+?)'!(\w+?)(\d+)"
        matched_list = [0,0,0]
        r = re.findall(pattern, rangeRef)
        matched_list[0] = r[0][0]
        matched_list[1] = int(r[0][2])
        matched_list[2] = ascii_uppercase.find(r[0][1])
        return matched_list

    @staticmethod
    def parse_dns_shop_entry(shop_entry):
        """Парсинг строки адреса ДНС.
        Возвращает list(Код, Название, Телефон, Время работы, Адрес, MD5(Адрес))
        """
        pattern = r"(М\d+)\s?—\s?([\s\S]+?), тел.([\s\S]+?)\s*\.\s*Режим работы:\s?([\s\S]+?)\s*\.\s*Адрес:\s?([\s\S]+)"
        r = re.findall(pattern, shop_entry)
        hash_object = hashlib.md5(str(r[0][4]).encode('utf-8'))
        return list(r[0]) + [hash_object.hexdigest()]