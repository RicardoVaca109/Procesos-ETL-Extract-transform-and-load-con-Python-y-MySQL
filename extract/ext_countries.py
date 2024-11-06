import traceback
import pandas as pd

def extraer_countries():
    try:
        filename = './csvs/countries.csv'
        countries = pd.read_csv(filename)
        return countries  # return del dataframe contiene todos los datos como tal
    except:
        traceback.print_exc()
    finally:
        pass
