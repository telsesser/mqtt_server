def diferencia_tiempos_segundos(tiempo_new_str, tiempo_old_str):
    tiempo_old_sec = time.mktime(time.strptime(tiempo_old_str, "%Y-%m-%d %H:%M:%S"))
    tiempo_new_sec = time.mktime(time.strptime(tiempo_new_str, "%Y-%m-%d %H:%M:%S"))
    return tiempo_new_sec - tiempo_old_sec


def get_mac_recurso(cur, mac):
    cur.execute(
        """SELECT id
        FROM monitors
        WHERE mac_address = %s""",
        (mac,),
    )
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute(
            """INSERT INTO monitors (mac_address)
            VALUES (%s)""",
            (mac,),
        )
        return cur.lastrowid


def insert_data_model_A(conn, data):
    sql_cursor = conn.cursor(buffered=True)
    id_monitor = get_mac_recurso(sql_cursor, data["MAC_rec"])
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
    if sql_cursor.rowcount == 0:
        median_rssi = 0
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


def insert_data_model_B(conn, data):
    pass
