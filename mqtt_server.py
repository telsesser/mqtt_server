import random
import time
import json
from paho.mqtt import client as mqtt_client
from queue import Queue
import sys
import logging
import csv
from bd import *


# MQTT CONFIG:
client = None
BROKER = "127.0.0.1"
# BROKER = "192.168.5.20"
PORT = 1883
client_id = f"python-mqtt-{random.randint(0, 1000)}"


# PILA DE DATOS DE ENTRADA
pila_MQTT = Queue()

# REVISADO
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
            print("CONECTADO AL BROKER")
            subscribe(client)
        else:
            print("Fallo la conexion con el BROKER. Codigo de error: %d", rc)
            # sys.exit(1)

    client = mqtt_client.Client(client_id)
    client.username_pw_set("local", "password")
    # client.username_pw_set(username, password)
    client.on_connect = on_connect

    while True:
        try:
            # print("BUSCANDO BROKER")
            client.connect(BROKER, PORT)
        except:
            # print("NO SE ENCUENTRA EL BROKER")
            time.sleep(1)
            continue
        else:
            # print("SE ENCONTRO EL BROKER!")
            break

    client.loop_start()
    return client


def procesar_datos_en_pila_mqtt():
    while True:
        if not pila_MQTT.empty():
            while not pila_MQTT.empty():
                msg = pila_MQTT.get()
                if msg is None:
                    continue
                try:
                    payload = json.loads(msg.payload)
                    topic = msg.topic
                    crearCursor()
                    insert_data(payload)
                    commit()


                except Exception as e:
                    rollback()
                    with open("datosNoProcesados.csv", "a") as f:
                        writer = csv.writer(f)
                        writer.writerow(
                            [
                                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                                msg.topic,
                                msg.payload,
                            ]
                        )

                    if msg.topic is not None and msg.payload is not None:
                        logging.exception(f"msg: {msg.payload} de topic: {msg.topic}")
                        break
                cerrarCursor()
        else:
            time.sleep(1)


def connect_mqtt_broker():
    global client
    client = connect_mqtt()