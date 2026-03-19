# ETL Órdenes de trabajo - Ejercicio Práctico

Ejercicio de proceso de extracción, transformación y carga de datos (ETL) para la ingesta diaria de órdenes de trabajo desde una base de datos MySQL y un fichero Excel, transformación de datos y carga en un DataMart accesible desde PowerBI

# Índice
1. [Descripción general](#descripción-general)
2. [Estructura del proyecto](#estructura-del-proyecto)
3. [Requisitos previos](#requisitos-previos)
4. [Instalación](#instalación)
5. [Configuración](#configuración)

---
## Descripción general
El proceso sigue una arquitectura clásica ETL donde se establecen las fuentes de datos (en este caso MySQL y un fichero Excel). Se transforma la data y luego se carga (load), para este caso se cargará en un DataMart para su posterior visualización en PowerBI.<br>

**Fuentes de datos:**
- **Origen 1 (MySQL)**: Tablas de órdenes de trabajo, actividad, técnicos y tipos de actividad.
- **Origen 2 (Excel)**: Fichero depositado diariamente en carpeta de red con el maestro de equipos.

**Destino:**
- **DataMart PostgreSQL**: Modelo estrella con tablas de hechos y dimensiones, listo para conectar con Power BI.
---
## Estructura del Proyecto
```
etl_diusframi/
│
├── config/
│   └── config.yaml             # Configuración de conexiones y parámetros del ETL
│
├── src/
│   ├── extractors/
│   │   ├── mysql_extractor.py  # Extrae datos de MySQL (carga incremental)
│   │   └── excel_extractor.py  # Lee el fichero Excel de equipos
│   │
│   ├── transformers/
│   │   └── data_transformer.py # Limpieza, validación y cálculo de métricas
│   │
│   ├── loaders/
│   │   └── datamart_loader.py  # Carga los datos en PostgreSQL (upsert)
│   │
│   └── utils/
│       ├── config_loader.py    # Carga el fichero config.yaml
│       ├── logger.py           # Configuración centralizada de logging
│       └── sla_calculator.py   # Cálculo de días hábiles (tiempo SLA)
│
├── sql/
│   └── create_datamart.sql     # DDL para crear las tablas del DataMart
├── logs/                       # Carpeta de logs (se crea automáticamente)
│
├── main.py                     # Punto de entrada del ETL
├── requirements.txt            # Dependencias Python
├── .env.example                # Plantilla para variables de entorno
└── README.md                   # Este fichero
```
## Requisitos previos
- **Python 3.10** o superior
- **PostgreSQL** o superior
- **MySQL 8** o superior
- Acceso a la carpeta de red donde se deposita el fichero Excel

## Instalación
### 1. Clonar repositorio
```bash
git clone https://github.com/jmleal2010/etl_workorders
cd etl_workorders
```

### 2. Crear un entorno virtual

```bash
# Crear el entorno virtual
python -m venv venv

# Activarlo (Windows)
venv\Scripts\activate

# Activarlo (Linux/Mac)
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Crear el fichero `.env`

```bash
# Copiar la plantilla
cp .env.example .env
```

### 5. Crear las tablas del DataMart

```bash
python main.py --init
```

Este comando ejecuta el fichero `sql/create_datamart.sql` en PostgreSQL y crea
todas las tablas necesarias.
---


## Configuración

Edita el fichero `config/config.yaml` con los datos de conexión:

```yaml
# Origen 1: MySQL
mysql:
  host: "localhost"
  port: 3306
  database: "factoria_db"
  user: "etl_user"
  password: "etl_password" 

# Origen 2: Excel
excel:
  path: "/ruta/carpeta_red/equipos.xlsx"
  sheet_name: "Equipos"

# Destino: PostgreSQL DataMart
datamart:
  host: "localhost"
  port: 5432
  database: "datamart_diusframi"
  user: "dm_user"
  password: "dm_password"   

# Parámetros del proceso
etl:
  fecha_inicio_historico: "2025-01-01"  
  batch_dias: 7                          
  pais_festivos: "ES"
  subdivision_festivos: "AN"           
```

> **Buena práctica de seguridad**: Definir las contraseñas en el fichero `.env`
> en lugar de en `config.yaml`. Las variables `MYSQL_PASSWORD` y `DATAMART_PASSWORD`
> sobreescriben los valores del YAML si están definidas.

---

## Cómo ejecutar el ETL


### Inicializar el DataMart

```bash
python main.py --init
# Inicializar el dataMart
```

### Ejecución normal (diaria)

```bash
python main.py
# Ejecución normal
```

El ETL determinará automáticamente el período a procesar basándose en la
última ejecución exitosa registrada en `etl_log`.

### Primera ejecución (carga histórica)

La primera vez que se ejecuta, el ETL detecta que no hay registros en `etl_log`
y carga todo el histórico desde `fecha_inicio_historico` (por defecto `2025-01-01`).

```bash
python main.py
# Primera ejecución detectada. Se cargará el histórico desde 2025-01-01.
```

### Usar un fichero de configuración alternativo

```bash
python main.py --config /ruta/alternativa/config.yaml
```

### Automatización diaria

Para ejecutar el ETL cada día automáticamente puedo dependiendo del Sistema Operativo, usar el programador de tareas en caso de estar en Windows o cron en caso de estar en Ubuntu:

**Windows (Programador de tareas):**
```
Programa: python
Argumentos: C:\ruta\etl\main.py
Inicio en: C:\ruta\etl
Frecuencia: Diaria, a la hora que se desee
```

**Linux (cron):**
```bash
# Editar crontab
crontab -e

# Ejecutar a las 06:00 todos los días
0 6 * * * cd /ruta/etl_diusframi && /ruta/venv/bin/python main.py >> logs/cron.log 2>&1
```

---

## Decisiones técnicas

### ¿Por qué Python + SQLAlchemy?

- Python es el estándar para proyectos ETL/Data Engineering.
- SQLAlchemy permite conectar a MySQL y PostgreSQL con el mismo código, cambiando solo la URL de conexión.
- Pandas facilita la manipulación y validación de datos.

### ¿Por qué PostgreSQL para el DataMart?

- Power BI tiene un conector nativo y gratuito para PostgreSQL.
- Es una solución robusta, gratuita y ampliamente usada en entornos corporativos.
- Soporta todos los tipos de datos necesarios y permite hacer consultas SQL directamente desde Power BI.

### ¿Por qué un modelo estrella?

El modelo estrella (fact tables + dimension tables) es el estándar para DataMarts porque:
- Simplifica las consultas de Power BI (menos JOINs complejos).
- Permite filtrar fácilmente por técnico, cliente, modelo o fecha.
- Es la arquitectura que los analistas de datos esperan encontrar.

### ¿Por qué upsert en lugar de INSERT simple?

El upsert (`INSERT ... ON CONFLICT DO UPDATE`) garantiza que el ETL sea **idempotente**:
si se ejecuta dos veces el mismo día, no se duplican datos.
Esto es especialmente importante para la recuperación ante fallos.

---

## Gestión de errores de calidad

El ETL detecta y gestiona dos tipos de errores de calidad (punto 4 del enunciado):

| Tipo de error         | Código           | Tratamiento                                            |
|-----------------------|------------------|--------------------------------------------------------|
| Falta `fecha_alta`    | `FECHA_ALTA_NULA`  | La orden se **descarta** y se registra en `etl_errores` |
| Falta `equipo_id`     | `EQUIPO_ID_NULO`   | La orden se **mantiene** sin equipo asociado            |
| Equipo no en Excel    | `EQUIPO_NO_EN_EXCEL` | La orden se **mantiene** con `equipo_id = NULL`       |

Los errores quedan registrados en la tabla `etl_errores` junto con el ID de la
ejecución que los detectó, para poder auditarlos desde Power BI o SQL.

---

## Recuperación ante fallos

El sistema está diseñado para recuperarse sin pérdida de datos si el proceso
no se ejecuta durante varios días (punto 6 del enunciado):

1. Cada ejecución queda registrada en `etl_log` con su estado (`en_proceso`, `completado`, `error`).
2. Al arrancar, el ETL consulta la última ejecución `completado` y retoma desde ahí.
3. Si hay muchos días pendientes, los procesa en **lotes de N días** (`batch_dias` en config.yaml)
   para no saturar el sistema con demasiados datos en memoria.
4. Si un lote falla, el proceso se detiene y la próxima ejecución retomará exactamente desde ese lote.

```
Ejemplo: El ETL no se ejecutó del lunes al miércoles.
El jueves arranca y detecta 3 días pendientes.
Con batch_dias=7, los procesa todos en un solo lote.
Si hubieran sido 20 días, los procesaría en lotes de 7 días.
```

---

## Logs

El ETL genera logs en la carpeta `logs/` con el nombre `etl_YYYY-MM-DD.log`.
Cada día tiene su propio fichero de log. Puedes ver los logs en tiempo real
mientras el ETL se ejecuta en la consola.

Ejemplo de salida:
```
2025-03-18 06:00:01 | INFO     | etl_main | ETL Diusframi - Inicio del proceso
2025-03-18 06:00:02 | INFO     | etl_main | --- Procesando lote: 2025-03-17 → 2025-03-18 ---
2025-03-18 06:00:03 | INFO     | etl_main | Extrayendo datos MySQL del 2025-03-17 al 2025-03-18...
2025-03-18 06:00:04 | INFO     | etl_main | Extracción MySQL completada: 12 órdenes, 34 actividades...
2025-03-18 06:00:05 | WARNING  | etl_main | Orden ORD-099: equipo 'EQ-77' no encontrado en el Excel.
2025-03-18 06:00:06 | INFO     | etl_main | Lote completado: 11 órdenes, 34 actividades, 1 errores de calidad.
2025-03-18 06:00:06 | INFO     | etl_main | Lotes completados: 1 | Lotes con error: 0
```
