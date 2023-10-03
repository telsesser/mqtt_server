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


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        # print(f"{msg.payload} en pila")
        pila_MQTT.put(msg)

    client.subscribe("/to_server/#")
    client.on_message = on_message


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to the broker")
            subscribe(client)
        else:
            logging.error("Failed to connect to the broker. Error code: %d", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set("local", "password")
    client.on_connect = on_connect

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


def procesar_datos_en_pila_mqtt():
    db = database.DatabasePool()
    while True:
        while not pila_MQTT.empty():
            start_time = time.perf_counter()
            msg = pila_MQTT.get()
            if msg.topic == "/to_server/gateway":
                continue
            try:
                payload = json.loads(msg.payload)
                match msg.topic:
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

            except Exception:
                if msg.topic and msg.payload:
                    logging.exception(f"{msg.topic}: {msg.payload}")
        time.sleep(1)


def connect_mqtt_broker():
    global client
    client = connect_mqtt()
