
import pandas as pd
import numpy as np
import os

from sqlalchemy import create_engine
from dotenv import load_dotenv  


# ========== CONFIGURACIÓN ==========
load_dotenv()
db_config = {
    'host': os.getenv("host"),
    'port': os.getenv("port"),
    'database': os.getenv("database"),
    'user': os.getenv("user"),
    'password': os.getenv("password"),
}

connection_str = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(connection_str)




# Leer el archivo Excel
df = pd.read_excel('ejmclasificaciones.xlsx')

nuevas_columnas = ['descripcion','concepto_ey','concepto_ey_global']


df.columns = nuevas_columnas



print(df)




# ========== MIGRAR A POSTGRESQL ==========
try:
    df.to_sql('ejmclasificaciones', engine, if_exists='replace', index=False)
    

    print("Datos insertados correctamente en la base de datos")

except Exception as e:
    print(f"Error al insertar los datos en la base de datos: {e}")
