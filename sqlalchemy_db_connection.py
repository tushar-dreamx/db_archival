from sqlalchemy import create_engine
from db_config import config


class PandasConnectionManager:
    con = None
    config = None

    def __init__(self):
        self.config = config

    def get_connection_engine(self):
        engine = create_engine(self.get_connection_url())
        return engine.connect()

    def get_connection_url(self):
        connection_type = "mysql+pymysql"
        connection_host = self.config['host']
        connection_user = self.config['user']
        connection_password = self.config['password']
        connection_database = self.config['database']
        return connection_type + "://" + connection_user + ':' + connection_password + '@' + connection_host + '/' + connection_database
