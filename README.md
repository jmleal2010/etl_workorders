# ETL Órdenes de trabajo - Ejercicio Práctico

Ejercicio de proceso de extracción, transformación y carga de datos (ETL) para la ingesta diaria de órdenes de trabajo desde una base de datos MySQL y un fichero Excel, transformación de datos y carga en un DataMart accesible desde PowerBI

# Índice
1. [Descripción general]()
2. [Estructura del proyecto]()
3. [Requisitos previos]()
4. [Instalación]()
5. [Configuración]()

---
## Descripción general
El proceso sigue una arquitectura clásica ETL donde se establecen las fuentes de datos (en este caso MySQL y un fichero Excel)> Se transforma la data y luego se carga (load), para este caso se cargará en un DataMart para su posterior visualización en PowerBI.<br>

**Fuentes de datos:**
- **Origen 1 (MySQL)**: Tablas de órdenes de trabajo, actividad, técnicos y tipos de actividad.
- **Origen 2 (Excel)**: Fichero depositado diariamente en carpeta de red con el maestro de equipos.

**Destino:**
- **DataMart PostgreSQL**: Modelo estrella con tablas de hechos y dimensiones, listo para conectar con Power BI.
---