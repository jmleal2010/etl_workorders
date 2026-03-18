
import os
import yaml
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    # Cargar variables de entorno si existen
    load_dotenv()

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"No se encontró el fichero de configuración: {config_path}\n"
            "Comprueba que existe y que la ruta es correcta."
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Sobreescribir contraseñas con variables de entorno si están definidas
    if os.getenv("MYSQL_PASSWORD"):
        config["mysql"]["password"] = os.getenv("MYSQL_PASSWORD")
        logger.debug("Contraseña MySQL cargada desde variable de entorno.")

    if os.getenv("DM_PASSWORD"):
        config["datamart"]["password"] = os.getenv("DM_PASSWORD")
        logger.debug("Contraseña DataMart cargada desde variable de entorno.")

    return config
