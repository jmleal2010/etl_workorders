# ETL Órdenes de trabajo - Ejercicio Práctico

Ejercicio de proceso de extracción, transformación y carga de datos (ETL) para la ingesta diaria de órdenes de trabajo desde una base de datos MySQL y un fichero Excel, transformación de datos y carga en un DataMart accesible desde PowerBI

# Índice
1. [Descripción general](#descripción-general)
2. [Estructura del proyecto](#estructura-del-proyecto)
3. [Requisitos previos](#requisitos-previos)
4. [Instalación](#)
5. [Configuración]()

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