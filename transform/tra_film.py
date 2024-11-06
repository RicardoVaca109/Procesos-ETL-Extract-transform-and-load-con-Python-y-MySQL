import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_film():
    try:
        type = 'mysql'
        host = '****'
        port = '****'
        user = '****'
        pwd = '****'
        db = 'staging'
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt = "SELECT \
                        film_id, \
                        title, \
                        description, \
                        release_year, \
                        language_id, \
                        original_language_id, \
                        rental_duration, \
                        rental_rate, \
                        CASE \
                        WHEN length < 60 THEN '< 1h' \
                        WHEN length >= 60 AND length < 90 THEN '< 1.5h' \
                        WHEN length >= 90 AND length < 120 THEN '< 2h' \
                        ELSE '> 2h' \
                        END AS length, \
                        replacement_cost, \
                        rating, \
                        special_features, \
                        last_update \
                    FROM ext_film"         
        stores_tra =  pd.read_sql (sql_stmt, ses_db)
        return stores_tra
    except:
        traceback.print_exc()
    finally:
        pass