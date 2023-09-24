import time
import json
import mariadb
from datetime import datetime

# SQL CONFIG:
# BD_IP = "192.168.5.20"
BD_IP = "127.0.0.1"
BD_NAME = "data"
BD_USER = "python"
BD_PSWRD = "holapython48"
sql_cursor = None
conn = None


def sql_start(data_base):
    # Connect to MariaDB Platform
    sql_conection_flag = False
    while not sql_conection_flag:
        try:
            conn = mariadb.connect(
                user=BD_USER,
                password=BD_PSWRD,
                host=BD_IP,
                port=3306,
                database=data_base,
            )
            sql_conection_flag = True

        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            # sys.exit(1)
            time.sleep(1)
            continue
        else:
            # print("CONECTADO A BD")
            break

    return conn


def is_connected():
    try:
        conn.ping()
    except:
        connect_bd()


def crearCursor():
    global sql_cursor
    is_connected()
    sql_cursor = conn.cursor(buffered=True)


def cerrarCursor():
    sql_cursor.close()


def commit():
    conn.commit()


def rollback():
    conn.rollback()


def connect():
    # global conn
    conn = sql_start(BD_NAME)
    conn.autocommit = False
    return conn


class DatabasePool:
    def __init__(self, db_name=BD_NAME):
        self.pool = mariadb.ConnectionPool(
            pool_name="mypool",
            pool_size=5,
            pool_validation_interval=500,
            host=BD_IP,
            port=3306,
            user=BD_USER,
            password=BD_PSWRD,
            database=db_name,
            autocommit=False,
        )

    def get_connection(self):
        return self.pool.get_connection()

    def __del__(self):
        self.pool.close()
