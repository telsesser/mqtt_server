import mqtt_server
import logging


def main():
    logging.basicConfig(
        filename="info.log",
        encoding="utf-8",
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    mqtt_server.connect_mqtt_broker()
    mqtt_server.procesar_datos_en_pila_mqtt()


if __name__ == "__main__":
    main()
