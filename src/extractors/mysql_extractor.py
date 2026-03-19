from datetime import date
from typing import Tuple
import logging

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class MysqlExtractor:
    def __init__(self, config: dict):
        self.config = config
        self.engine = None

    def _connect(self):
        from sqlalchemy.engine import URL
        url = URL.create(
            drivername="mysql+pymysql",
            username=self.config["user"],
            password=self.config["password"],
            host=self.config["host"],
            port=int(self.config["port"]),
            database=self.config["database"],
            query={"charset": "utf8mb4"},
        )
        self.engine = create_engine(url)
        logger.debug("Conexión a MySQL establecida.")

    def _disconnect(self):
        if self.engine:
            self.engine.dispose()
            logger.debug("Conexión a MySQL cerrada")

    def _extract_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        query = text("""
                     SELECT o.num_orden,
                            o.fecha_alta,
                            o.identificador_equipo,
                            o.estado,
                            o.fecha_cierre
                     FROM ordenes_trabajo o
                     WHERE o.num_orden IN (

                         SELECT DISTINCT a.num_orden
                         FROM actividad a
                         WHERE a.fecha_hora_actividad >= :start_date
                           AND a.fecha_hora_actividad < :end_date)
                     """)
        df = pd.read_sql(
            query,
            self.engine,
            params={"start_date": start_date, "end_date": end_date},
        )

        logger.debug(f"Órdenes extraídas: {len(df)}")
        return df

    def _extract_activities(self, start_date: date, end_date: date) -> pd.DataFrame:
        query = text("""
                     SELECT a.identificador_tecnico,
                            a.num_orden,
                            a.fecha_hora_actividad,
                            a.tipo_actividad,
                            a.comentario
                     FROM actividad a
                     WHERE a.num_orden IN (SELECT DISTINCT num_orden
                                           FROM actividad
                                           WHERE fecha_hora_actividad >= :fecha_desde
                                             AND fecha_hora_actividad < :fecha_hasta)
                     """)

        df = pd.read_sql(
            query,
            self.engine,
            params={"fecha_desde": start_date, "fecha_hasta": end_date},
        )
        logger.debug(f"Registros de actividad extraídos: {len(df)}")
        return df
    
    def _extract_technicians(self) -> pd.DataFrame:
 
        query = text("SELECT identificador_tecnico, nombre FROM tecnicos")
        df = pd.read_sql(query, self.engine)
        logger.debug(f"Técnicos extraídos: {len(df)}")
        return df

    def extract_activity_types(self) -> pd.DataFrame:
        query = text("SELECT identificador_actividad, nombre FROM tipos_actividad")
        df = pd.read_sql(query, self.engine)
        logger.debug(f"Tipos de actividad extraídos: {len(df)}")
        return df
    
    def extract(
            self,
            start_date: date,
            end_date: date,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        try:
            self._connect()

            logger.info(f"Extrayendo datos MysSQL del ${start_date} al ${end_date}");
            orders_df = self._extract_orders(start_date, end_date)
            activities_df = self._extract_activities(start_date, end_date)
            technicians_df = self._extract_technicians()
            activities_types_df = self.extract_activity_types()

            return orders_df, activities_df, technicians_df, activities_types_df
        except Exception as e:
            logger.error(f"Error al extraer datos de MySQL: {e}", exc_info=True)
            raise
        finally:
            self._disconnect()
