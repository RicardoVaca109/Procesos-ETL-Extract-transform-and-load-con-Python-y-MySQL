**Proyecto ETL con Python y MySQL - Base de Datos Sakila**

Este proyecto implementa un proceso ETL (Extract, Transform, Load) en Python para extraer datos de la base de datos Sakila, transformarlos y 
cargarlos en un área de almacenamiento intermedio o destino específico. El proceso usa Python y Pandas para realizar la manipulación de datos, 
y una conexión MySQL para interactuar con la base de datos.

**Objetivo:**
Extrae datos de la base de datos Sakila.
Transforma los datos de acuerdo con los requisitos específicos (e.g., unificación de datos, creación de valores calculados).
Carga los datos transformados en tablas de un área de almacenamiento (staging) o una base de datos de reportes (sor).

**Requisitos**
-Python 3.x
-Base de datos MySQL: Tener acceso a una base de datos MySQL con la base de datos Sakila.

**Librerías de Python:** 
-pip install pandas mysql-connector-python

**Configurar la conexión en util/db_connection.py:**
Modifica los parámetros de conexión para conectar tu base de datos MySQL.

**Ejecutar cada proceso ETL:**
  -Extracción de direcciones: Ejecutar extraer_addresses.py.
  -Transformación de datos de tiendas: Ejecutar transformar_store.py.
  -Carga de datos de fechas: Ejecutar carga_dates.py
