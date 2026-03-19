
from datetime import date
from typing import List, Optional
import logging

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class DataMartLoader:

    def __init__(self, config: dict):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self):
        from sqlalchemy.engine import URL
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=self.config["user"],
            password=self.config["password"],
            host=self.config["host"],
            port=int(self.config["port"]),
            database=self.config["database"],
        )
        return create_engine(url)

    def get_last_successful_run_date(self) -> Optional[date]:
        query = text("""
            SELECT fecha_hasta
            FROM etl_log
            WHERE estado = 'completado'
            ORDER BY fecha_hasta DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            resultado = conn.execute(query).fetchone()

        if resultado:
            return resultado[0]
        return None

    def registrar_inicio_ejecucion(self, fecha_desde: date, fecha_hasta: date) -> int:

        query = text("""
            INSERT INTO etl_log (fecha_desde, fecha_hasta, estado)
            VALUES (:fecha_desde, :fecha_hasta, 'en_proceso')
            RETURNING id
        """)
        with self.engine.connect() as conn:
            resultado = conn.execute(
                query,
                {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta}
            ).fetchone()
            conn.commit()

        ejecucion_id = resultado[0]
        logger.debug(f"Ejecución registrada con ID: {ejecucion_id}")
        return ejecucion_id

    def registrar_fin_ejecucion(
        self,
        ejecucion_id: int,
        estado: str,
        ordenes: int = 0,
        actividades: int = 0,
        num_errores: int = 0,
        detalle: str = None,
    ):
        query = text("""
            UPDATE etl_log
            SET estado                 = :estado,
                ordenes_procesadas     = :ordenes,
                actividades_procesadas = :actividades,
                registros_con_error    = :num_errores,
                detalle                = :detalle
            WHERE id = :ejecucion_id
        """)
        with self.engine.connect() as conn:
            conn.execute(query, {
                "estado": estado,
                "ordenes": ordenes,
                "actividades": actividades,
                "num_errores": num_errores,
                "detalle": detalle,
                "ejecucion_id": ejecucion_id,
            })
            conn.commit()

    def guardar_errores(self, ejecucion_id: int, errores: List[dict]):
        if not errores:
            return

        query = text("""
            INSERT INTO etl_errores (ejecucion_id, orden_id, tipo_error, detalle)
            VALUES (:ejecucion_id, :orden_id, :tipo_error, :detalle)
        """)
        with self.engine.connect() as conn:
            for error in errores:
                conn.execute(query, {
                    "ejecucion_id": ejecucion_id,
                    "orden_id": error.get("orden_id"),
                    "tipo_error": error["tipo_error"],
                    "detalle": error.get("detalle"),
                })
            conn.commit()

        logger.info(f"Guardados {len(errores)} errores de calidad en etl_errores.")

    def load(
        self,
        fact_ordenes: pd.DataFrame,
        fact_actividad: pd.DataFrame,
        dim_equipo: pd.DataFrame,
        dim_tecnico: pd.DataFrame,
        dim_tipo: pd.DataFrame,
    ):

        logger.info("Iniciando carga al DataMart...")

        self._upsert(dim_equipo, "dim_equipo", "equipo_id")
        self._upsert(dim_tecnico, "dim_tecnico", "tecnico_id")
        self._upsert(dim_tipo, "dim_tipo_actividad", "tipo_actividad_id")
        self._upsert(fact_ordenes, "fact_ordenes", "orden_id")
        self._upsert(fact_actividad, "fact_actividad", "actividad_id")

        logger.info("Carga al DataMart completada.")

    def _upsert(self, df: pd.DataFrame, table_name: str, pk_column: str):
        if df is None or df.empty:
            logger.debug(f"Sin datos para cargar en {table_name}. Se omite.")
            return

        temp_table = f"tmp_{table_name}"
        columnas = list(df.columns)
        columnas_update = [c for c in columnas if c != pk_column]

        with self.engine.begin() as conn:

            df.to_sql(
                name=temp_table,
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
            )

            set_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in columnas_update
            )
            upsert_sql = text(f"""
                INSERT INTO {table_name} ({", ".join(columnas)})
                SELECT {", ".join(columnas)} FROM {temp_table}
                ON CONFLICT ({pk_column})
                DO UPDATE SET {set_clause}
            """)
            conn.execute(upsert_sql)

            # Paso 3: eliminar tabla temporal
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

        logger.debug(f"Upsert completado en {table_name}: {len(df)} registros.")

    def initialize_datamart(self, sql_path: str = "sql/create_datamart.sql"):
        try:
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_full = f.read()

            statements = sql_full.split(';')

            with self.engine.begin() as conn:
                for statement in statements:
                    clean_statement = statement.strip()

                    if clean_statement:
                        conn.execute(text(clean_statement))

            logger.info("DataMart inicializado correctamente.")

        except Exception as e:
            logger.error(f"Error al inicializar el DataMart: {e}")
            raise
