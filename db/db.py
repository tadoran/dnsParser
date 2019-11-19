from mysql.connector import MySQLConnection, Error
from config.config import Configuration


class DatabaseConnection():
    __shared_state = {}  # Borg design pattern, shared state

    def read_db_config(self, section='mysql'):
        config = Configuration()
        return getattr(config, section)

    def __init__(self):
        self.__dict__ = self.__shared_state
        db_config = self.read_db_config()
        # db_config.append()
        try:
            print('Connecting to MySQL database...')
            self.conn = MySQLConnection(**db_config)

            if self.conn.is_connected():
                print('connection established.')
                self.cursor = self.conn.cursor(dictionary=True)
                self.requests_pending = 0
            else:
                print('connection failed.')

        except Error as error:
            print(error)

    def execute(self, *args):
        self.cursor.execute(*args)
        self.conn.commit()

    def executemany(self, *args):
        self.cursor.executemany(*args)
        self.conn.commit()

    def iter_row(self, size=10):
        while True:
            rows = self.cursor.fetchmany(size)
            if not rows:
                break
            for row in rows:
                yield row

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

    def put_new_cities_to_db(self, values):
        print(values)
        for name, hash_str in values:
            print(f"INSERT IGNORE INTO cities (`city_name`,`city_hash`) VALUES ({name},{hash_str})")
        query = "INSERT IGNORE INTO cities (`city_name`,`city_hash`) VALUES (%s,%s)"
        return self.executemany(query, values)

    def get_all_cities(self, size=10):
        query = "SELECT * FROM cities"
        self.cursor.execute(query)
        return self.iter_row(size)

    def get_all_cities_wo_filename(self, size=10):
        query = "SELECT * FROM cities WHERE cities.city_name_DNS IS NULL"  # limit 10,3"
        self.cursor.execute(query)
        return self.iter_row(size)

    def get_shops_id_by_md5(self, md5_list, size=10):
        format_strings = ','.join(['%s'] * len(md5_list))
        query = self.cursor.execute("""SELECT id, MD5 FROM shops WHERE MD5 IN (%s)""" % format_strings, tuple(md5_list))
        return self.iter_row(size)

    def put_shops_to_db(self, values):
        # TODO: Test
        query = "INSERT IGNORE INTO shops (MD5, Name, Phone, worktime, Address) VALUES (%s, %s, %s, %s, %s)"
        self.cursor.executemany(query, tuple(values))
        self.conn.commit()
        return self.cursor.fetchone()

    def get_devices_not_in_db(self, articles):
        # TODO: Test
        format_strings = ','.join(['%s'] * len(articles))
        self.cursor.execute("SELECT device_article in devices WHERE device_article IN (%s)" % format_strings,
                            tuple(articles))

        articles_available = [val for val in self.cursor.fetchall().values()]
        return [article for article in articles if article not in articles_available]

    def put_devices_to_db(self, *values):
        # TODO: Test
        query = "INSERT IGNORE INTO devices (device_article, device_name_dns) VALUES (%s, %s)"
        self.cursor.executemany(query, tuple(values))
        self.conn.commit()
        return self.cursor.fetchone()

    def put_availability_to_db(self, *values):
        # TODO: Test
        query = "INSERT IGNORE INTO `dnsavailability`.`availability` (`device_article`,`price`,`prozaPass`,`shop`," \
                "`date`) VALUES (%s,%s,%s,%s,%s); "
        self.cursor.executemany(query, tuple(values))
        self.requests_pending += 1

        if self.requests_pending >= 10:
            self.requests_pending = 0
            self.conn.commit()

             # self.cursor.fetchone()
            return self.cursor.rowcount

    def put_dns_names_to_db(self, *values):
        query = ("UPDATE IGNORE cities "
                 "SET "
                 "city_name_DNS = %s, "
                 "city_name_file = %s, "
                 "city_archive = %s "
                 "WHERE "
                 "cities_id = %s"
                 )
        self.cursor.execute(query, tuple(values))
        self.conn.commit()
        return self.cursor.fetchone()
