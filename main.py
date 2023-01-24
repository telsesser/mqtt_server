import mqtt_server
import logging
import bd


def main():
    # logging.basicConfig(filename='errors.log', encoding='utf-8', level=logging.DEBUG)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler("errors.log", "w", "utf-8")
    root_logger.addHandler(handler)
    mqtt_server.connect_mqtt_broker()
    bd.connect_bd()
    mqtt_server.procesar_datos_en_pila_mqtt()


if __name__ == "__main__":
    main()