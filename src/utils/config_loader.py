
import os
import logging
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Mapeo completo: variable de entorno → sección.clave del config

ENV_MAPPING = {
    # MySQL
    "MYSQL_HOST":     ("mysql",     "host",     str),
    "MYSQL_PORT":     ("mysql",     "port",     int),
    "MYSQL_DATABASE": ("mysql",     "database", str),
    "MYSQL_USER":     ("mysql",     "user",     str),
    "MYSQL_PASSWORD": ("mysql",     "password", str),
    # Excel
    "EXCEL_PATH":     ("excel",     "path",         str),
    "EXCEL_SHEET":    ("excel",     "sheet_name",   str),
    # DataMart PostgreSQL
    "DATAMART_HOST":     ("datamart", "host",     str),
    "DATAMART_PORT":     ("datamart", "port",     int),
    "DATAMART_DATABASE": ("datamart", "database", str),
    "DATAMART_USER":     ("datamart", "user",     str),
    "DATAMART_PASSWORD": ("datamart", "password", str),
}


def load_config(config_path: str = "config/config.yaml") -> dict:
    # 1. Cargar el .env (si existe) antes de leer os.getenv
    load_dotenv()

    # 2. Leer el YAML como base
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"No se encontró el fichero de configuración: {config_path}"
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 3. Sobreescribir con variables de entorno si están definidas
    for env_var, (seccion, clave, tipo) in ENV_MAPPING.items():
        valor_env = os.getenv(env_var)
        if valor_env is not None:
            # Convertir al tipo correcto (p.ej. PORT es int)
            try:
                config[seccion][clave] = tipo(valor_env)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"La variable de entorno {env_var}='{valor_env}' "
                    f"no se puede convertir a {tipo.__name__}: {e}"
                )
            logger.debug(f"Config [{seccion}.{clave}] cargado desde {env_var}.")

    # 4. Validar que los campos obligatorios no están vacíos
    _validar_config(config)

    return config


def _validar_config(config: dict):

    obligatorios = [
        # (seccion, clave, variable_de_entorno_esperada)
        ("mysql",    "user",     "MYSQL_USER"),
        ("mysql",    "password", "MYSQL_PASSWORD"),
        ("excel",    "path",     "EXCEL_PATH"),
        ("datamart", "user",     "DATAMART_USER"),
        ("datamart", "password", "DATAMART_PASSWORD"),
    ]

    errores = []
    for seccion, clave, env_var in obligatorios:
        valor = config.get(seccion, {}).get(clave, "")
        if not valor:
            errores.append(f"  - {env_var}  →  config[{seccion}][{clave}]")

    if errores:
        raise ValueError(
            "Faltan las siguientes variables de entorno obligatorias.\n"
            "Defínelas en el fichero .env (copia .env.example como guía):\n"
            + "\n".join(errores)
        )