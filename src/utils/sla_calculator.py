
from datetime import date, timedelta
from typing import Optional
import holidays
import logging

logger = logging.getLogger(__name__)


def calculate_sla_time(
    start_date: date,
    end_date: date,
    country: str = "ES",
    region: str = "AN",
) -> Optional[int]:

    if start_date is None or end_date is None:
        return None

    # Si las fechas estan mal ordenadas (fecha de cierre mayor a la del alta) devolvemos cero
    if end_date <= start_date:
        return 0

    # Recopilamos los dias festivos dentro del rango
    festivos = set()
    for anio in range(start_date.year, end_date.year + 1):
        try:
            festivos_anio = holidays.country_holidays(country, subdiv=region, years=anio)
            festivos.update(festivos_anio.keys())
        except Exception as e:
            logger.warning(f"No se pudieron cargar festivos del año {anio}: {e}")

    # Conteo de dias hábiles
    dias_habiles = 0
    dia_actual = start_date

    while dia_actual < end_date:
        # Si es mayor o igual a 5 fin de semana
        es_fin_de_semana = dia_actual.weekday() >= 5
        es_festivo = dia_actual in festivos

        if not es_fin_de_semana and not es_festivo:
            dias_habiles += 1

        dia_actual += timedelta(days=1)

    return dias_habiles


def calculate_total_time(
    start_date: date,
    end_date: date,
) -> Optional[int]:

    if start_date is None or end_date is None:
        return None

    if end_date <= start_date:
        return 0

    return (end_date - start_date).days

