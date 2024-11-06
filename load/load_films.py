from transform.tra_film import transformar_film 
import traceback
from util.db_connection import Db_Connection
import pandas as pd

def carga_films():
    try:
        type = 'mysql'
        host = '****'
        port = '****'
        user = '****'
        pwd = '****'
        db = 'staging'
        films_df = transformar_film()
        if films_df is None or films_df.empty:
            raise Exception("No hay datos de películas para cargar.")

        db = 'sor'
        con_sor_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido.")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos.")
        dim_film_df = films_df[['film_id', 'title', 'release_year', 'length', 'rating', 'duration']].copy()
        dim_film_df.rename(columns={'film_id': 'film_bk'}, inplace=True)

        dim_film_df['film_bk'] = dim_film_df['film_bk'].astype('int16')
        dim_film_df['release_year'] = dim_film_df['release_year'].astype('int')
        dim_film_df['length'] = dim_film_df['length'].astype('int16')
        dim_film_df['rating'] = dim_film_df['rating'].astype(str)
        dim_film_df['duration'] = dim_film_df['duration'].astype(str)
        dim_film_df.to_sql('dim_film', ses_sor_db, if_exists='append', index=False)
    except Exception as e:
        traceback.print_exc()
    finally:
        pass