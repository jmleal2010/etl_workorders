
DROP TABLE IF EXISTS etl_errores      CASCADE;
DROP TABLE IF EXISTS etl_log          CASCADE;
DROP TABLE IF EXISTS fact_actividad   CASCADE;
DROP TABLE IF EXISTS fact_ordenes     CASCADE;
DROP TABLE IF EXISTS dim_tipo_actividad CASCADE;
DROP TABLE IF EXISTS dim_tecnico      CASCADE;
DROP TABLE IF EXISTS dim_equipo       CASCADE;


CREATE TABLE dim_equipo (
    equipo_id         VARCHAR(50)  PRIMARY KEY,
    modelo            VARCHAR(100),
    cliente           VARCHAR(150),
    fecha_actualizacion TIMESTAMP  DEFAULT NOW()
);

COMMENT ON TABLE dim_equipo IS 'Maestro de equipos a reparar. Se carga desde el fichero Excel diario.';
COMMENT ON COLUMN dim_equipo.equipo_id IS 'Identificador del equipo (clave del Excel).';



CREATE TABLE dim_tecnico (
    tecnico_id        VARCHAR(50)  PRIMARY KEY,
    nombre            VARCHAR(150),
    fecha_actualizacion TIMESTAMP  DEFAULT NOW()
);

COMMENT ON TABLE dim_tecnico IS 'Maestro de tecnicos. El nombre puede cambiar con el tiempo (se guarda el mas reciente).';



CREATE TABLE dim_tipo_actividad (
    tipo_actividad_id VARCHAR(50)  PRIMARY KEY,
    nombre            VARCHAR(150),
    fecha_actualizacion TIMESTAMP  DEFAULT NOW()
);

COMMENT ON TABLE dim_tipo_actividad IS 'Maestro de tipos de actividad realizables en una orden.';

CREATE TABLE fact_ordenes (
    orden_id              VARCHAR(50)  PRIMARY KEY,

    fecha_alta            DATE         NOT NULL,
    fecha_cierre          DATE,

    equipo_id             VARCHAR(50)  REFERENCES dim_equipo(equipo_id),

    estado                VARCHAR(20)  NOT NULL
                          CHECK (estado IN ('nueva', 'en curso', 'cerrada')),

    tiempo_total_trabajo  INT,
    tiempo_sla            INT,


    fecha_actualizacion   TIMESTAMP    DEFAULT NOW()
);

COMMENT ON TABLE fact_ordenes IS 'Tabla principal de hechos. Contiene las ordenes con sus metricas calculadas.';
COMMENT ON COLUMN fact_ordenes.tiempo_total_trabajo IS 'Dias naturales entre alta y cierre de la orden.';
COMMENT ON COLUMN fact_ordenes.tiempo_sla IS 'Dias habiles (sin fines de semana ni festivos) entre alta y cierre.';


CREATE TABLE fact_actividad (
    actividad_id        VARCHAR(50)  PRIMARY KEY,
    orden_id            VARCHAR(50)  NOT NULL REFERENCES fact_ordenes(orden_id),
    tecnico_id          VARCHAR(50)  REFERENCES dim_tecnico(tecnico_id),
    tipo_actividad_id   VARCHAR(50)  REFERENCES dim_tipo_actividad(tipo_actividad_id),
    fecha_actividad     TIMESTAMP    NOT NULL,
    comentario          TEXT,

    -- Auditoría ETL
    fecha_actualizacion TIMESTAMP    DEFAULT NOW()
);

COMMENT ON TABLE fact_actividad IS 'Detalle de todas las acciones realizadas por tecnicos sobre cada orden.';


CREATE TABLE etl_log (
    id                  SERIAL       PRIMARY KEY,
    fecha_ejecucion     TIMESTAMP    DEFAULT NOW(),
    fecha_desde         DATE         NOT NULL,   -- Inicio del rango procesado
    fecha_hasta         DATE         NOT NULL,   -- Fin del rango procesado
    estado              VARCHAR(20)  NOT NULL
                        CHECK (estado IN ('en_proceso', 'completado', 'error')),
    ordenes_procesadas  INT          DEFAULT 0,
    actividades_procesadas INT       DEFAULT 0,
    registros_con_error INT          DEFAULT 0,
    detalle             TEXT         -- Mensaje adicional o traza de error
);

COMMENT ON TABLE etl_log IS 'Control de ejecuciones del ETL. Usado para cargas incrementales y recuperacion ante fallos.';

CREATE TABLE etl_errores (
    id              SERIAL       PRIMARY KEY,
    ejecucion_id    INT          REFERENCES etl_log(id),
    orden_id        VARCHAR(50),
    tipo_error      VARCHAR(100) NOT NULL,
    detalle         TEXT,
    fecha_registro  TIMESTAMP    DEFAULT NOW()
);

COMMENT ON TABLE etl_errores IS 'Cuarentena de registros con errores de calidad de datos durante el ETL.';


CREATE INDEX idx_fact_ordenes_fecha_alta  ON fact_ordenes(fecha_alta);
CREATE INDEX idx_fact_ordenes_estado      ON fact_ordenes(estado);
CREATE INDEX idx_fact_ordenes_equipo_id   ON fact_ordenes(equipo_id);

CREATE INDEX idx_fact_actividad_orden_id   ON fact_actividad(orden_id);
CREATE INDEX idx_fact_actividad_tecnico_id ON fact_actividad(tecnico_id);
CREATE INDEX idx_fact_actividad_fecha      ON fact_actividad(fecha_actividad);

CREATE INDEX idx_etl_log_fecha_estado ON etl_log(fecha_hasta, estado);

CREATE OR REPLACE VIEW v_metricas_diarias AS
SELECT
    fecha_alta                                             AS fecha,
    COUNT(*)                                               AS volumen_total,
    SUM(CASE WHEN estado = 'en curso' THEN 1 ELSE 0 END)  AS volumen_en_curso,
    SUM(CASE WHEN estado = 'cerrada'  THEN 1 ELSE 0 END)  AS volumen_finalizadas
FROM fact_ordenes
GROUP BY fecha_alta;

-- =============================================================================
-- VISTAS PARA POWER BI
-- Las métricas de volumen son simples COUNT sobre fact_ordenes.
-- No hace falta una tabla extra: Power BI consulta estas vistas directamente.
-- =============================================================================

CREATE OR REPLACE VIEW v_metricas_mensuales AS
SELECT
    EXTRACT(YEAR  FROM fecha_alta)::INT                    AS anio,
    EXTRACT(MONTH FROM fecha_alta)::INT                    AS mes,
    COUNT(*)                                               AS volumen_total,
    SUM(CASE WHEN estado = 'en curso' THEN 1 ELSE 0 END)  AS volumen_en_curso,
    SUM(CASE WHEN estado = 'cerrada'  THEN 1 ELSE 0 END)  AS volumen_finalizadas
FROM fact_ordenes
GROUP BY anio, mes;