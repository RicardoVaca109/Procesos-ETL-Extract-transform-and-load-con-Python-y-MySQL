import traceback
from util.db_connection import Db_Connection
import pandas as pd

def persisitir_staging(df_stg, tab_name):
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
        
        if 'last_update' in df_stg.columns:
            df_stg['last_update'] = pd.to_datetime(df_stg['last_update'], errors='coerce')
        df_stg.to_sql(tab_name, ses_db, if_exists = 'replace', index = False)
        
    except:
        traceback.print_exc()
    finally:
        pass