import time
import json
import mariadb
from datetime import datetime

# SQL CONFIG:
# BD_IP = "192.168.5.20"
BD_IP = "127.0.0.1"
BD_NAME = "oee_db"
BD_USER = "python"
BD_PSWRD = "holapython48"
sql_cursor = None
conn = None

def diferencia_tiempos_segundos(tiempo_new_str, tiempo_old_str):
    tiempo_old_sec = time.mktime(time.strptime(tiempo_old_str, "%Y-%m-%d %H:%M:%S"))
    tiempo_new_sec = time.mktime(time.strptime(tiempo_new_str, "%Y-%m-%d %H:%M:%S"))
    return tiempo_new_sec - tiempo_old_sec


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


def crearCursor():
    global sql_cursor
    sql_cursor = conn.cursor(buffered=True)


def cerrarCursor():
    sql_cursor.close()


def commit():
    conn.commit()


def rollback():
    conn.rollback()


def connect_bd():
    global conn
    conn = sql_start(BD_NAME)
    conn.autocommit = False