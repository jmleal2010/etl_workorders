import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


class ExcelExtractor:
    def __init__(self, config: dict):
        self.path = config['path']
        self.sheet = config.get('sheet_name', "Equipos")

    def extract(self) -> pd.DataFrame:
        if not os.path.exists(self.path):
            raise FileNotFoundError(
                f"No se pudo encontrar el fichero Excel en : {self.path}\n"
                "Comprueba que existe el fichero"
            )

        logger.info(f"Leyendo fichero Excel: {self.path} (hoja: {self.sheet})")

        try:
            df = pd.read_excel(self.path, sheet_name=self.sheet)
        except Exception as e:
            raise ValueError(f"Error leyendo el fichero Excel: {e}") from e

        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        required_columns = {'identificador_equipo', 'modelo', 'cliente'}
        current_columns = set(df.columns)

        if not required_columns.issubset(current_columns):
            missing_columns = required_columns - current_columns
            raise ValueError(
                f"El fichero Excel no tiene las columnas requeridas: {missing_columns}\n"
                f"Columnas encontradas: {current_columns}"
            )

        #Eliminamos las columnas completamente vacías
        df = df.dropna(how="all")

        for col in ["identificador_equipo", "modelo", "cliente"]:
            df[col] = df[col].astype(str).str.strip()

        logger.info(f"Equipos leídos del Excel: {len(df)}")
        return df