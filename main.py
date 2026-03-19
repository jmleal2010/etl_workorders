from datetime import date, timedelta
import logging
import sys
import argparse

from src.extractors.excel_extractor import ExcelExtractor
from src.extractors.mysql_extractor import MysqlExtractor
from src.loaders.datamart_loader import DataMartLoader
from src.transformers.data_transformer import DataTransformer
from src.utils.config_loader import load_config
from src.utils.logger import setup_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL de órdenes de trabajo - Ejercicio Práctico Diusframi"
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Inicializa el DataMart (crea las tablas en PostgreSQL) y sale.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Ruta al fichero de configuración (por defecto: config/config.yaml).",
    )
    return parser.parse_args()


def process_batch(
        start_date: date,
        end_date: date,
        config: dict,
        loader: DataMartLoader,
        logger: logging.Logger,
) -> bool:
    """
    Ejecuta el ETL completo para un período concreto (un lote).

    Esta función encapsula una ejecución de Extraer → Transformar → Cargar
    para el rango [fecha_desde, fecha_hasta). Se usa tanto en ejecuciones
    normales (un día) como en la recuperación de días perdidos (varios días).

    Args:
        start_date: Inicio del período (inclusive).
        end_date: Fin del período (exclusive).
        config: Configuración completa del ETL.
        loader: Instancia del cargador al DataMart.
        logger: Logger activo.

    Returns:
        True si el lote se procesó con éxito, False si hubo error.
    """
    logger.info(f"--- Procesando lote: {start_date} → {end_date} ---")

    # Registrar inicio en etl_log (permite recuperación si falla a mitad)
    ejecucion_id = loader.registrar_inicio_ejecucion(start_date, end_date)

    try:
        # ---------------------------------------------------------------
        # FASE 1: EXTRACCIÓN
        # ---------------------------------------------------------------
        mysql_ext = MysqlExtractor(config["mysql"])
        excel_ext = ExcelExtractor(config["excel"])

        ordenes_df, actividad_df, tecnicos_df, tipos_df = mysql_ext.extract(
            start_date, end_date
        )
        equipos_df = excel_ext.extract()

        if ordenes_df.empty:
            logger.info("No hay órdenes con actividad en este período. Lote completado.")
            loader.registrar_fin_ejecucion(
                ejecucion_id, "completado", detalle="Sin datos en el período."
            )
            return True

        # ---------------------------------------------------------------
        # FASE 2: TRANSFORMACIÓN
        # ---------------------------------------------------------------
        transformer = DataTransformer(config.get("etl", {}))
        fact_ordenes, fact_actividad, dim_equipo, dim_tecnico, dim_tipo, errores = (
            transformer.transform(
                ordenes_df, actividad_df, tecnicos_df, tipos_df, equipos_df
            )
        )

        # ---------------------------------------------------------------
        # FASE 3: CARGA
        # ---------------------------------------------------------------
        loader.load(fact_ordenes, fact_actividad, dim_equipo, dim_tecnico, dim_tipo)

        # Guardar errores de calidad detectados
        if errores:
            loader.guardar_errores(ejecucion_id, errores)

        # Marcar ejecución como completada
        loader.registrar_fin_ejecucion(
            ejecucion_id,
            estado="completado",
            ordenes=len(fact_ordenes),
            actividades=len(fact_actividad),
            num_errores=len(errores),
        )

        logger.info(
            f"Lote completado: {len(fact_ordenes)} órdenes, "
            f"{len(fact_actividad)} actividades, {len(errores)} errores de calidad."
        )
        return True

    except Exception as e:
        logger.error(f"Error procesando lote {start_date}→{end_date}: {e}", exc_info=True)
        loader.registrar_fin_ejecucion(
            ejecucion_id,
            estado="error",
            detalle=str(e),
        )
        return False


def main():
    args = parse_args()
    logger = setup_logger("etl")
    logger.info("=" * 60)
    logger.info("  ETL Ejercicio Práctico Diusframi - Inicio del proceso")
    logger.info("=" * 60)

    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(0)

    loader = DataMartLoader(config['datamart'])

    if args.init:
        logger.info("Inicializando DataMart (creando tablas)...")
        loader.initialize_datamart()
        logger.info("DataMart listo. Ahora puedes ejecutar el ETL sin --init.")
        sys.exit(0)

    fecha_historica_str = config.get("etl", {}).get("fecha_inicio_historico", "2025-01-01")
    fecha_historica = date.fromisoformat(fecha_historica_str)

    ultima_ejecucion = loader.get_last_successful_run_date()

    if ultima_ejecucion is None:
        # Primera vez que se ejecuta el ETL (punto 7)
        logger.info(
            f"Primera ejecución detectada. "
            f"Se cargará el histórico desde {fecha_historica}."
        )
        fecha_desde = fecha_historica
    else:
        # Ejecuciones siguientes: arrancamos desde donde quedamos
        fecha_desde = ultima_ejecucion + timedelta(days=1)

    fecha_hasta = date.today()

    if fecha_desde > fecha_hasta:
        logger.info("El DataMart ya está al día. No hay datos nuevos que procesar.")
        sys.exit(0)

    # -----------------------------------------------------------------------
    # Procesamiento por lotes (punto 6: recuperación ante fallos)
    #
    # Si el proceso lleva N días sin ejecutarse, dividimos el período en
    # lotes de `batch_dias` días para no sobrecargar el sistema con
    # demasiados datos en memoria a la vez.
    # -----------------------------------------------------------------------
    batch_dias = config.get("etl", {}).get("batch_dias", 7)
    dias_totales = (fecha_hasta - fecha_desde).days + 1

    if dias_totales > batch_dias:
        logger.info(
            f"Se procesarán {dias_totales} días en lotes de {batch_dias} días "
            f"para no sobrecargar el sistema."
        )

    lote_desde = fecha_desde
    lotes_ok = 0
    lotes_error = 0

    while lote_desde <= fecha_hasta:
        lote_hasta = min(lote_desde + timedelta(days=batch_dias), fecha_hasta + timedelta(days=1))

        exito = process_batch(lote_desde, lote_hasta, config, loader, logger)

        if exito:
            lotes_ok += 1
        else:
            lotes_error += 1
            # Si un lote falla, detenemos el proceso.
            # La próxima ejecución retomará desde este lote (gracias a etl_log).
            logger.error(
                "Se ha detenido el proceso por un error en el lote. "
                "La próxima ejecución retomará desde aquí."
            )
            break

        lote_desde = lote_hasta

    # -----------------------------------------------------------------------
    # Resumen final
    # -----------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info(f"  Lotes completados: {lotes_ok}")
    logger.info(f"  Lotes con error:   {lotes_error}")
    logger.info("=" * 60)

    if lotes_error > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
