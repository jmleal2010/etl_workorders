from datetime import datetime
from typing import Tuple, List
import logging

import pandas as pd

from src.utils.sla_calculator import calculate_sla_time, calculate_total_time

logger = logging.getLogger(__name__)

def create_error(order_id: str, error_type: str, details: str) -> dict:
    return {
        "orden_id": order_id,
        "tipo_error": error_type,
        "detalle": details,
    }

class DataTransformer:
    def __init__(self, config: dict):
        self.country = config.get('festivity_country', "ES")
        self.region = config.get('festivity_region', "AN")

    def transform(
            self,
            orders_df: pd.DataFrame,
            activities_df: pd.DataFrame,
            technicians_df: pd.DataFrame,
            activities_types_df: pd.DataFrame,
            equipments_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, List[dict]]:

        errores: List[dict] = []
        dim_equipo = self._prepare_equipment_dim(equipments_df)
        dim_tecnico = self._prepare_technician_dim(technicians_df)
        dim_types = self._prepare_activities_types_dim(activities_types_df)

        clean_orders, errors_orders = self.clean_and_validate_orders(
            orders_df, equipments_df
        )
        errores.extend(errors_orders)

        fact_ordenes = self._calculate_metrics(clean_orders)
        fact_activities = self._prepare_activity(activities_df, clean_orders)

        return fact_ordenes, fact_activities, dim_equipo, dim_tecnico, dim_types, errores

    def _prepare_equipment_dim(self, equipments_df: pd.DataFrame) -> pd.DataFrame:
        if equipments_df.empty:
            return pd.DataFrame(columns=["equipo_id", "modelo", "cliente"])

        dim = equipments_df[["identificador_equipo", "modelo", "cliente"]].copy()
        dim = dim.rename(columns={"identificador_equipo": "equipo_id"})
        dim = dim.drop_duplicates(subset=["equipo_id"])
        dim["fecha_actualizacion"] = datetime.now()

        return dim

    def _prepare_technician_dim(self, tecnicos_df: pd.DataFrame) -> pd.DataFrame:

        if tecnicos_df.empty:
            return pd.DataFrame(columns=["tecnico_id", "nombre"])

        dim = tecnicos_df.rename(
            columns={"identificador_tecnico": "tecnico_id"}
        ).copy()
        dim = dim.drop_duplicates(subset=["tecnico_id"])
        dim["fecha_actualizacion"] = datetime.now()
        return dim

    def _prepare_activities_types_dim(self, types_df: pd.DataFrame) -> pd.DataFrame:
        if types_df.empty:
            return pd.DataFrame(columns=["tipo_actividad_id", "nombre"])

        dim = types_df.rename(
            columns={"identificador_actividad": "tipo_actividad_id"}
        ).copy()
        dim = dim.drop_duplicates(subset=["tipo_actividad_id"])
        dim["fecha_actualizacion"] = datetime.now()
        return dim

    def clean_and_validate_orders(
            self,
            orders_df: pd.DataFrame,
            equipments_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, List[dict]]:

        errors = []
        if orders_df.empty:
            return pd.DataFrame(), errors

        df = orders_df.copy()

        # ---- Error 4a: Falta fecha_alta ----
        not_entry_date = df['fecha_alta'].isna()
        for order_id in df.loc[not_entry_date, "num_orden"]:
            logger.warning(f"Orden {order_id}: falta fecha_alta. Se descarta.")
            errors.append(create_error(
                order_id,
                "FECHA_ALTA_NULA",
                "La orden no tiene fecha de alta y no puede procesarse."
            ))

        df = df[~not_entry_date].copy()

        # ---- Error 4a: Falta identificador_equipo ----
        without_equipment = df["identificador_equipo"].isna() | (df["identificador_equipo"].astype(str).str.strip() == "")
        for orden_id in df.loc[without_equipment, "num_orden"]:
            logger.warning(f"Orden {orden_id}: falta identificador_equipo.")
            errors.append(create_error(
                orden_id,
                "EQUIPO_ID_NULO",
                "La orden no tiene identificador de equipo."
            ))
        # En este punto no descartarmos la orden, sigue siendo valida, pero sin equipo asociado
        df.loc[without_equipment, "identificador_equipo"] = None

        # ---- Error 4b: equipo_id no existe en el Excel ----
        if not equipments_df.empty:
            equipos_validos = set(equipments_df["identificador_equipo"].astype(str))
            con_equipo = df["identificador_equipo"].notna()

            not_in_excel = con_equipo & ~df["identificador_equipo"].astype(str).isin(equipos_validos)
            for _, row in df[not_in_excel].iterrows():
                logger.warning(
                    f"Orden {row['num_orden']}: equipo '{row['identificador_equipo']}' "
                    f"no encontrado en el Excel."
                )
                errors.append(create_error(
                    row["num_orden"],
                    "EQUIPO_NO_EN_EXCEL",
                    f"El equipo '{row['identificador_equipo']}' no está en el maestro de equipos."
                ))
            # Seteamos el equipo a None para no romper el foreign_key del dataMart
            df.loc[not_in_excel, "identificador_equipo"] = None

        # Convertir fechas a tipo date
        df["fecha_alta"] = pd.to_datetime(df["fecha_alta"]).dt.date
        df["fecha_cierre"] = pd.to_datetime(df["fecha_cierre"], errors="coerce").dt.date

        return df, errors

    def _prepare_activity(
            self,
            activity_df: pd.DataFrame,
            valid_orders_df: pd.DataFrame,
    ) -> pd.DataFrame:

        if activity_df.empty or valid_orders_df.empty:
            return pd.DataFrame()

        ordenes_validas_ids = set(valid_orders_df["num_orden"])

        df = activity_df[
            activity_df["num_orden"].isin(ordenes_validas_ids)
        ].copy()

        df["actividad_id"] = (
                df["identificador_tecnico"].astype(str) + "_"
                + df["num_orden"].astype(str) + "_"
                + df["fecha_hora_actividad"].astype(str)
        ).str.replace(" ", "T")

        df = df.rename(columns={
            "identificador_tecnico": "tecnico_id",
            "num_orden": "orden_id",
            "fecha_hora_actividad": "fecha_actividad",
            "tipo_actividad": "tipo_actividad_id",
        })

        df["fecha_actualizacion"] = datetime.now()

        columnas_finales = [
            "actividad_id", "orden_id", "tecnico_id",
            "tipo_actividad_id", "fecha_actividad", "comentario", "fecha_actualizacion"
        ]
        return df[columnas_finales]

    def _calculate_metrics(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        if orders_df.empty:
            return pd.DataFrame()

        df = orders_df.copy()

        # Cambio de las columnas al modelo de dataMart
        df = df.rename(columns={
            "num_orden": "orden_id",
            "identificador_equipo": "equipo_id",
        })

        # Inicializar métricas a None
        df["tiempo_total_trabajo"] = None
        df["tiempo_sla"] = None

        # Calcular solo para órdenes cerradas
        closed_orders = df["estado"] == "cerrada"
        logger.debug(f"Calculando métricas para {closed_orders.sum()} órdenes cerradas.")

        for idx in df[closed_orders].index:
            start_date = df.at[idx, "fecha_alta"]
            end_date = df.at[idx, "fecha_cierre"]

            df.at[idx, "tiempo_total_trabajo"] = calculate_total_time(
                start_date, end_date
            )
            df.at[idx, "tiempo_sla"] = calculate_sla_time(
                start_date, end_date,
                country=self.country,
                region=self.region,
            )

        df["tiempo_total_trabajo"] = df["tiempo_total_trabajo"].astype("Int64")
        df["tiempo_sla"] = df["tiempo_sla"].astype("Int64")

        df["fecha_actualizacion"] = datetime.now()

        # Seleccionar columnas finales
        columnas_finales = [
            "orden_id", "fecha_alta", "fecha_cierre", "equipo_id",
            "estado", "tiempo_total_trabajo", "tiempo_sla", "fecha_actualizacion"
        ]
        return df[columnas_finales]