import os
from configparser import ConfigParser


class Configuration:
    __shared_state = {}  # Borg design pattern, shared state

    def __init__(self, filename='config.ini'):
        self.__dict__ = self.__shared_state

        if hasattr(self, 'sections') is False:
            self.sections = []
            self.read_db_config(filename)
        else:

    def read_db_config(self, filename='config.ini'):
        """ Read database configuration file and return a dictionary object
        :param filename: name of the configuration file
        :param section: section of database configuration
        :return: a dictionary of database parameters
        """

        folder = r"C:\Users\Gorelov\Documents\DNS PyParsing"
        path_to_file = os.path.join(folder, filename)

        # create parser and read ini configuration file
        parser = ConfigParser()
        parser.read(path_to_file)

        for config_section in parser.sections():
            self.sections.append(config_section)
            setattr(self, config_section, {})
            items = parser.items(config_section)
            for item in items:
                getattr(self, config_section)[item[0]] = item[1]
