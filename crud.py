import csv
import time


def diferencia_tiempos_segundos(tiempo_new_str, tiempo_old_str):
    tiempo_old_sec = time.mktime(time.strptime(tiempo_old_str, "%Y-%m-%d %H:%M:%S"))
    tiempo_new_sec = time.mktime(time.strptime(tiempo_new_str, "%Y-%m-%d %H:%M:%S"))
    return tiempo_new_sec - tiempo_old_sec


def get_monitor_info(cur, mac):
    cur.execute(
        """SELECT id, battery, openings
        FROM monitors
        WHERE mac_address = %s""",
        (mac,),
    )
    result = cur.fetchone()
    if result:
        monitor = {
            "id": result[0],
            "battery": result[1],
            "openings": result[2],
        }
    else:
        cur.execute(
            """INSERT INTO monitors (mac_address)
            VALUES (%s)""",
            (mac,),
        )
        monitor = {
            "id": cur.lastrowid,
            "battery": None,
            "openings": None,
        }
    return monitor


def insert_data_model_A(db, data):
    conn = db.get_connection()
    sql_cursor = conn.cursor(buffered=True)
    try:
        monitor = get_monitor_info(sql_cursor, data["MAC_rec"])
        valores = (
            monitor["id"],
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
        sql_cursor.execute(query_median, (monitor["id"],))
        if sql_cursor.rowcount == 0:
            median_rssi = abs(data["rssi"])
        else:
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
                monitor["id"],
            ),
        )

        if median_rssi - 1 <= abs(data["rssi"]) <= median_rssi + 1:
            query = """UPDATE monitors SET moved=?, moved_datetime=? WHERE id=?"""
            sql_cursor.execute(
                query,
                (
                    True,
                    data["timestmp"],
                    monitor["id"],
                ),
            )

        # Obtener el último nivel de batería registrado
        query = """SELECT battery_level FROM monitors_battery WHERE id_monitor = ? ORDER BY timestmp DESC LIMIT 1"""
        sql_cursor.execute(query, (monitor["id"],))
        last_battery_level = sql_cursor.fetchone()

        # Comprobar si el nivel de batería ha cambiado o si "id_battery" es NULL
        if last_battery_level is None or last_battery_level[0] < data["bat"]:
            # Registrar el nuevo valor de batería en "monitors_battery"
            query = """INSERT INTO monitors_battery (battery_level, timestmp, id_monitor) VALUES (?,?,?)"""
            sql_cursor.execute(query, (data["bat"], data["timestmp"], monitor["id"]))

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def insert_data_model_B(db, data):
    conn = db.get_connection()
    sql_cursor = conn.cursor(buffered=True)
    try:
        monitor = get_monitor_info(sql_cursor, data["MAC_rec"])

        query = f"""INSERT INTO data 
                (id_monitor, temp, openings, rssi, timestmp)
                VALUES (?,?,?,?,?)"""
        values = (
            monitor["id"],
            data["temp"],
            data["n_apert"],
            abs(data["rssi"]),
            data["timestmp"],
        )
        sql_cursor.execute(query, values)

        # Update the "monitores" table with the latest values
        query = """UPDATE monitors SET battery=?, rssi=?, openings=?, last_data=?, temp=? WHERE id=?"""
        values = (
            data["bat"],
            abs(data["rssi"]),
            data["n_apert"] - monitor["openings"],
            data["timestmp"],
            data["temp"],
            monitor["id"],
        )
        sql_cursor.execute(query, values)

        # Obtener la mediana de rssi de los últimos 24 registros
        # TODO: Hacer con over partition

        query_median = """SELECT MEDIAN(rssi) OVER () FROM (
            SELECT rssi
            FROM data
            WHERE id_monitor = ?
            ORDER BY timestmp DESC
            LIMIT 24
        ) AS subquery"""
        sql_cursor.execute(query_median, (monitor["id"],))

        if sql_cursor.rowcount == 0:
            median_rssi = abs(data["rssi"])
        else:
            median_rssi = sql_cursor.fetchone()[0]

        if median_rssi - 20 <= abs(data["rssi"]) <= median_rssi + 20:
            query = """UPDATE monitors SET moved=?, moved_datetime=? WHERE id=?"""
            sql_cursor.execute(
                query,
                (
                    True,
                    data["timestmp"],
                    monitor["id"],
                ),
            )

        # Comprobar si el nivel de batería ha cambiado o si "id_battery" es NULL
        if (monitor["battery"] is None) or (data["bat"] < monitor["battery"]):
            # Registrar el nuevo valor de batería en "monitors_battery"
            query = """INSERT INTO monitors_battery (battery_level, timestmp, id_monitor) VALUES (?,?,?)"""
            sql_cursor.execute(query, (data["bat"], data["timestmp"], monitor["id"]))

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_data_s3(data):
    with open("unprocessed-door-openings.csv", "a") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                data["timestmp"],
                data["press[0]"],
                data["press[1]"],
                data["press[2]"],
                data["press[3]"],
                data["press[4]"],
                data["press[5]"],
                data["press[6]"],
                data["press[7]"],
            ]
        )
