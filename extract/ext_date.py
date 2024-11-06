import traceback
import pandas as pd

def extraer_dates():
    try:
        filename = './csvs/dates.csv'
        dates = pd.read_csv(filename)
        return dates  # return del dataframe contiene todos los datos como tal
    except:
        traceback.print_exc()
    finally:
        pass
