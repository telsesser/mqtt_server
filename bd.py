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


def diferencia_tiempos_segundos(tiempo_new_str, tiempo_old_str):
    tiempo_old_sec = time.mktime(time.strptime(tiempo_old_str, "%Y-%m-%d %H:%M:%S"))
    tiempo_new_sec = time.mktime(time.strptime(tiempo_new_str, "%Y-%m-%d %H:%M:%S"))
    return tiempo_new_sec - tiempo_old_sec


def get_mac_recurso(mac):
    sql_cursor.execute(
        f"""SELECT id
        FROM monitors
        WHERE mac_address = '{mac}'"""
    )
    result = sql_cursor.fetchone()
    if result is not None:
        return result[0]
    else:
        sql_cursor.execute(
            f"""INSERT INTO monitors (mac_address)
            VALUES ('{mac}')"""
        )
        return sql_cursor.lastrowid


# {"timestmp":"2023-05-05 13:58:04",
# "MAC_rec":"d8c7f5157ba7",
# "tipo":1,
# "temp":1.070000,
# "n_apert":0,
# "rssi":-73,
# "bat":80}


def insert_data(data):
    id_monitor = get_mac_recurso(data["MAC_rec"])
    valores = (
        id_monitor,
        data["temp"],
        data["n_apert"],
        abs(data["rssi"]),
        data["timestmp"],
    )

    # Obtener la mediana de rssi de los últimos 24 registros
    query_median = """SELECT MEDIAN(rssi) OVER () FROM (
        SELECT rssi
        FROM data
        WHERE id_monitor = ?
        ORDER BY timestmp DESC
        LIMIT 24
    ) AS subquery"""
    sql_cursor.execute(query_median, (id_monitor,))
    median_rssi = sql_cursor.fetchone()[0]

    query = f"""INSERT INTO data 
            (id_monitor, temp, openings, rssi, timestmp)
            VALUES (?,?,?,?,?)"""
    sql_cursor.execute(query, valores)

    # Update the "monitores" table with the latest values
    query = """UPDATE monitors SET battery=?, rssi=?, openings=?, last_data=?, temp=? WHERE id=?"""
    sql_cursor.execute(
        query,
        (
            data["bat"],
            abs(data["rssi"]),
            data["n_apert"],
            data["timestmp"],
            data["temp"],
            id_monitor,
        ),
    )

    if median_rssi - 1 <= abs(data["rssi"]) <= median_rssi + 1:
        query = """UPDATE monitors SET moved=?, moved_datetime=? WHERE id=?"""
        sql_cursor.execute(
            query,
            (
                True,
                data["timestmp"],
                id_monitor,
            ),
        )

    # Obtener el último nivel de batería registrado
    query = """SELECT battery_level FROM monitors_battery WHERE id_monitor = ? ORDER BY timestmp DESC LIMIT 1"""
    sql_cursor.execute(query, (id_monitor,))
    last_battery_level = sql_cursor.fetchone()

    # Comprobar si el nivel de batería ha cambiado o si "id_battery" es NULL
    if last_battery_level is None or last_battery_level[0] < data["bat"]:
        # Registrar el nuevo valor de batería en "monitors_battery"
        query = """INSERT INTO monitors_battery (battery_level, timestmp, id_monitor) VALUES (?,?,?)"""
        sql_cursor.execute(query, (data["bat"], data["timestmp"], id_monitor))


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


def connect_bd():
    global conn
    conn = sql_start(BD_NAME)
    conn.autocommit = False
