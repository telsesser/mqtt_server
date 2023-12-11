import csv
import time
import os


def diferencia_tiempos_segundos(tiempo_new_str, tiempo_old_str):
    tiempo_old_sec = time.mktime(time.strptime(tiempo_old_str, "%Y-%m-%d %H:%M:%S"))
    tiempo_new_sec = time.mktime(time.strptime(tiempo_new_str, "%Y-%m-%d %H:%M:%S"))
    return tiempo_new_sec - tiempo_old_sec


def get_monitor_info(cur, mac):
    cur.execute(
        """SELECT id, battery, openings, id_refrigerator
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
            "id_refrigerator": result[3],
        }
    else:
        cur.execute(
            """INSERT INTO monitors (mac_address)
            VALUES (%s)""",
            (mac,),
        )
        monitor = {
            "id": cur.lastrowid,
            "battery": 100,
            "openings": 0,
            "id_refrigerator": None,
        }
    return monitor


def insert_data_model_A(db, data):
    conn = db.get_connection()
    sql_cursor = conn.cursor(buffered=True)
    try:
        monitor = get_monitor_info(sql_cursor, data["MAC_rec"])
        valores = (
            monitor["id"],
            monitor["id_refrigerator"],
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
                (id_monitor,id_refrigerator, temp, openings, rssi, timestmp)
                VALUES (?,?,?,?,?,?)"""
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

        last_three_rssis = []
        query = """SELECT rssi FROM data WHERE id_monitor = ? ORDER BY timestmp DESC LIMIT 3;"""
        sql_cursor.execute(query, (monitor["id"],))
        last_three_rssis = sql_cursor.fetchall()

        if all(abs(median_rssi - rssi) > 20 for rssi in last_three_rssis):
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
        sql_cursor.execute(
            """SELECT id_monitor FROM data WHERE id_monitor = %s AND msg_counter = %s""",
            (monitor["id"], data["count"]),
        )
        if sql_cursor.rowcount == 0:
            data["rssi"] = abs(data["rssi"])
            query = f"""INSERT INTO data 
                    (id_monitor,id_refrigerator,msg_counter, temp, openings, rssi, timestmp)
                    VALUES (?,?,?,?,?,?,?)"""
            values = (
                monitor["id"],
                monitor["id_refrigerator"],
                data["count"],
                data["temp"],
                data["n_apert"],
                data["rssi"],
                data["timestmp"],
            )
            sql_cursor.execute(query, values)

            query = """UPDATE monitors SET rssi=?, openings=?, last_data=?, temp=? WHERE id=?"""
            values = (
                data["rssi"],
                data["n_apert"],
                data["timestmp"],
                data["temp"],
                monitor["id"],
            )
            sql_cursor.execute(query, values)

            query_median = """SELECT MEDIAN(rssi) OVER () FROM (
                SELECT rssi
                FROM data
                WHERE id_monitor = ?
                ORDER BY timestmp DESC
                LIMIT 24
            ) AS subquery"""
            sql_cursor.execute(query_median, (monitor["id"],))

            if sql_cursor.rowcount == 0:
                median_rssi = data["rssi"]
            else:
                median_rssi = sql_cursor.fetchone()[0]

            if abs(median_rssi - data["rssi"]) > 20:
                query = """UPDATE monitors SET moved=?, moved_datetime=? WHERE id=?"""
                sql_cursor.execute(
                    query,
                    (
                        True,
                        data["timestmp"],
                        monitor["id"],
                    ),
                )

            if data["bat"] < monitor["battery"]:
                query = """UPDATE monitors SET battery=? WHERE id=?"""
                sql_cursor.execute(query, (data["bat"], monitor["id"]))
                query = """INSERT INTO monitors_battery (battery_level, timestmp, id_monitor) VALUES (?,?,?)"""
                sql_cursor.execute(
                    query, (data["bat"], data["timestmp"], monitor["id"])
                )

            conn.commit()
        else:
            raise Exception("Dato duplicado")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


import os


def insert_data_s3(data):
    file_exists = os.path.isfile("unprocessed-door-openings.csv")
    with open("unprocessed-door-openings.csv", "a") as f:
        writer = csv.writer(f)
        if not file_exists:
            header = [
                "timestmp",
                "MAC_rec",
                "classification_value",
                "press[0]",
                "press[1]",
                "press[2]",
                "press[3]",
                "press[4]",
                "press[5]",
                "press[6]",
                "press[7]",
            ]

            writer.writerow(header)
        writer.writerow(
            [
                data["timestmp"],
                data["MAC_rec"],
                data["classification_value"],
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
