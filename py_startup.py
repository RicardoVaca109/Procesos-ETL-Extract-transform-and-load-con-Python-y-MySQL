# this file is a kind of python startup module used for manual unit testing

from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_store
from util.db_connection import Db_Connection
from extract.per_staging import persisitir_staging
from transform.tra_stores import transformar_store
from extract.ext_city import extraer_city
from extract.ext_adress import extraer_addresses
from extract.ext_date import extraer_dates
from extract.ext_film import extraer_film
from extract.ext_inventory import extraer_inventory
from transform.tra_film import transformar_film
from load.load_dates import carga_dates
from load.load_films import carga_films
from load.load_inventory import cargar_factory_inventory
from load.load_stores import load_store
import traceback
import pandas as pd

try:
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    print("Extrayendo datos desde countries desde un CSV")
    countries = extraer_countries()
    
    print("Persistiendo en Staging datos de Countries")
    persisitir_staging(countries, 'ext_country')
    print("Tabla Countries:\n", countries)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    print("Extrayendo datos store desde la DB")
    stores = extraer_store()
    # print("Data de tabla store:\n", stores)
    print("Persistiendo en Staging datos de stores")
    persisitir_staging(stores, 'ext_store')
    print("Tabla Stores:\n", stores)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    print("Extrayendo datos city desde la DB ")
    cities = extraer_city()
    
    print("Persistiendo en Staging datos de city")
    persisitir_staging(cities, 'ext_city')
    print("Tabla Cities:\n", cities)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    print("Extrayendo datos address desde la DB")
    addresses = extraer_addresses()
    
    print("Persistiendo datos en Staging de address")
    persisitir_staging(addresses, 'ext_address')
    print("Tabla Addresses:\n", addresses)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    print("Extrayendo datos de dates desde un CSV")
    dates = extraer_dates()
    print("Persistiendo en Staging datos de Dates")
    persisitir_staging(dates, 'ext_date')
    print("Tabla Dates:\n", dates)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    print("Extrayendo datos film desde la DB")
    film = extraer_film()
    print("Persisitiendo datos de film en el Staging")
    persisitir_staging(film, 'ext_film')
    print("Tabla Film:\n", film)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    print("Extrayendo datos de inventory desde la DB")
    inventory = extraer_inventory()
    print("Persistiendo datos inventory en Staging")
    persisitir_staging(inventory, 'ext_inventory')
    print("Tabla Inventory:\n",inventory)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    
    print("Transformando datos store en el staging ")
    tra_stores = transformar_store()
    print("Transformando:\n",tra_stores)
    
    print("Persistir datos tranformados Store en Staging")
    persisitir_staging(tra_stores, 'tra_store')
    print("Tabla ",tra_stores)
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    
    print("Transformando datos film en el Staging")
    tra_films = transformar_film()
    
    print("Persistir datos transformados Film en Staging")
    persisitir_staging(tra_films, 'tra_film')
    print("Tabla Film:\n", tra_films)
    
    
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    print("Cargando datos de Store en SOR")
    load_store()
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    print("Cargando datos de Dates en SOR")
    carga_dates()
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    print("Cargando datos de Inventory en SOR")
    cargar_factory_inventory()
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-")
    print("Cargando datos de Films en SOR")
    carga_films()

except:
    traceback.print_exc()
finally:
    pass
