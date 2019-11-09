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
        query = "INSERT IGNORE INTO cities (`city_name`,`city_hash`) VALUES (%s,%s)"
        self.executemany(query, values)

    def get_all_cities(self, size=10):
        query = "SELECT * FROM cities"
        self.cursor.execute(query)
        return self.iter_row(size)

    def get_all_cities_wo_filename(self, size=10):
        query = "SELECT * FROM cities WHERE cities.city_name_DNS IS NULL"  # limit 10,3"
        self.cursor.execute(query)
        return self.iter_row(size)

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
