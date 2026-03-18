
import logging
import os
from datetime import datetime


def setup_logger(name: str = "etl", log_dir: str = "logs") -> logging.Logger:

    # Creo la carpeta de logs si no existe
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)

    # Evito agregar handlers duplicados por si se llama varias veces
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Formato de los mensajes de log
    formato = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler de consola (muestra INFO y superior)
    handler_consola = logging.StreamHandler()
    handler_consola.setLevel(logging.INFO)
    handler_consola.setFormatter(formato)

    # Handler de fichero (muestra DEBUG y superior, más detallado)
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    ruta_fichero = os.path.join(log_dir, f"etl_{fecha_hoy}.log")
    handler_fichero = logging.FileHandler(ruta_fichero, encoding="utf-8")
    handler_fichero.setLevel(logging.DEBUG)
    handler_fichero.setFormatter(formato)

    logger.addHandler(handler_consola)
    logger.addHandler(handler_fichero)

    return logger
