import time
import json
from paho.mqtt import client as mqtt_client
from queue import Queue
import logging
import csv
import database
import crud

# MQTT CONFIG:
client = None
BROKER = "127.0.0.1"
# BROKER = "192.168.5.20"
PORT = 1883
client_id = f"python-mqtt-server"


# PILA DE DATOS DE ENTRADA
pila_MQTT = Queue()


def mqtt_publish_check(result, msg):
    status = result[0]
    if status == 0:
        # print(f"Send msg: {msg} ")
        None
    else:
        print(f"Failed to send: {msg}")


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to the broker")
            client.subscribe("/to_server/#", qos=1)
        else:
            logging.error(f"Failed to connect to the broker. Error code: {rc}")

    def on_disconnect(client, userdata, rc):
        if rc != 0:
            logging.error(f"Unexpected disconnection. Error code: {rc}")
        else:
            logging.info(f"Disconnected from the broker. Error code: {rc}")

    def on_message(client, userdata, msg):
        pila_MQTT.put(msg)

    client = mqtt_client.Client(client_id=client_id, clean_session=False)
    client.username_pw_set("local", "password")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    # client.enable_logger(logger=logging.getLogger())

    while True:
        try:
            client.connect(BROKER, PORT)
        except Exception as e:
            time.sleep(1)
            continue
        else:
            break

    client.loop_start()
    return client


import re


def corregir_json(json_str):
    return re.sub(
        r'("id_gateway":\s*)("[^"]+"|[^,}\s]+)',
        lambda match: (
            match.group(1) + '"\2"' if '"' not in match.group(2) else match.group(0)
        ),
        json_str,
    )


def procesar_datos_en_pila_mqtt():
    db = database.DatabasePool()
    while True:
        while not pila_MQTT.empty():
            start_time = time.perf_counter()
            msg = pila_MQTT.get()
            if msg.topic == "/to_server/gateway":
                continue
            try:
                json_str = corregir_json(msg.payload.decode("utf-8"))
                payload = json.loads(json_str)
                topic = msg.topic.replace(" ", "")
                match topic:
                    case "/to_server/refrigerators/model_B/status":
                        crud.insert_status_model_B(db, payload)
                    case "/to_server/refrigerators/model_B":
                        crud.insert_data_model_B(db, payload)
                    case "/to_server/refrigerators/model_A":
                        crud.insert_data_model_A(db, payload)
                    case "/to_server/s3":
                        crud.insert_data_s3(payload)
                    case _:
                        raise Exception(f"Topic not recognized")
                end_time = time.perf_counter()
                processing_time = (end_time - start_time) * 1000
                logging.info(f"{msg.topic.split('/')[-1]} {processing_time:.6f} ms")

            except Exception as e:
                if e.args[0] == "Dato duplicado":
                    end_time = time.perf_counter()
                    processing_time = (end_time - start_time) * 1000
                    logging.info(f"Dato duplicado {processing_time:.6f} ms")
                elif msg.topic and msg.payload:
                    logging.exception(f"{e} {msg.topic}: {msg.payload}")
        time.sleep(1)


def connect_mqtt_broker():
    global client
    client = connect_mqtt()
