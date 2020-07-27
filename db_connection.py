import mysql.connector
from mysql.connector import Error
from db_config import config


class ConnectionManager:
    connection = None
    config = None

    def __init__(self):
        self.config = config
        if not self.connection:
            try:
                self.connection = mysql.connector.connect(**config)
                if self.connection.is_connected():
                    db_info = self.connection.get_server_info()
                    print("Connected to MySQL Server version ", db_info)

            except Error as e:
                print("Error while connecting to MySQL \n", e)
                raise e

    def get_connection(self):
        return self.connection

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("connection closed.")
