import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_factory_inventory():
    try:
        type = 'mysql'
        host = '****'
        port = '****'
        user = '****'
        pwd = '****'
        db_oltp = 'oltp'
        con_oltp_db = Db_Connection(type, host,     port, user, pwd, db_oltp)
        ses_oltp_db = con_oltp_db.start()
        if ses_oltp_db == -1:
            raise Exception("El tipo de base de datos dado no es válido.")
        elif ses_oltp_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos 'oltp'.")
        
        sql_inventory = "SELECT inventory_id, film_id, store_id, last_update FROM inventory"
        inventory_df = pd.read_sql(sql_inventory, ses_oltp_db)

        sql_film = "SELECT film_id, rental_rate, replacement_cost FROM film"
        film_df = pd.read_sql(sql_film, ses_oltp_db)

        inventory_film_df = pd.merge(inventory_df, film_df, on='film_id', how='left')
        db_sor = 'sor'
        con_sor_db = Db_Connection(type, host, port, user, pwd, db_sor)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido.")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos 'sor'.")

        sql_dim_film = "SELECT id AS film_dim_id, film_bk FROM dim_film"
        dim_film_df = pd.read_sql(sql_dim_film, ses_sor_db)

        sql_dim_store = "SELECT id AS store_dim_id, store_bk FROM dim_store"
        dim_store_df = pd.read_sql(sql_dim_store, ses_sor_db)

    
        sql_dim_date = "SELECT id AS date_id, date_bk FROM dim_date"
        dim_date_df = pd.read_sql(sql_dim_date, ses_sor_db)
        inventory_film_df = pd.merge(
            inventory_film_df, dim_film_df, left_on='film_id', right_on='film_bk', how='left')

        inventory_film_df = pd.merge(
            inventory_film_df, dim_store_df, left_on='store_id', right_on='store_bk', how='left')
        inventory_film_df['date_bk'] = pd.to_datetime(inventory_film_df['last_update']).dt.date
        inventory_film_df = pd.merge(
            inventory_film_df, dim_date_df, on='date_bk', how='left')
        fact_inventory_df = inventory_film_df[[
            'store_dim_id', 'film_dim_id', 'date_id', 'rental_rate', 'replacement_cost']].copy()
        fact_inventory_df.rename(columns={
            'store_dim_id': 'store_id',
            'film_dim_id': 'film_id',
            'rental_rate': 'rental_price',
            'replacement_cost': 'rental_cost'
        }, inplace=True)

        fact_inventory_df.dropna(subset=['store_id', 'film_id', 'date_id'], inplace=True)
      
        fact_inventory_df['store_id'] = fact_inventory_df['store_id'].astype(int)
        fact_inventory_df['film_id'] = fact_inventory_df['film_id'].astype(int)
        fact_inventory_df['date_id'] = fact_inventory_df['date_id'].astype(int)
        fact_inventory_df['rental_price'] = fact_inventory_df['rental_price'].astype(float)
        fact_inventory_df['rental_cost'] = fact_inventory_df['rental_cost'].astype(float)

        fact_inventory_df.to_sql('fact_inventory', ses_sor_db, if_exists='append', index=False)

    except Exception as e:
        traceback.print_exc()
    finally:
        pass
