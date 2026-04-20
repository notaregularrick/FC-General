
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

from Clasificador import Clasificarbalance
from searchenasientos import searchenasientos

# ========== CONFIG ==========
load_dotenv()
db_config = {
    'host': os.getenv("host"),
    'port': os.getenv("port"),
    'database': os.getenv("database"),
    'user': os.getenv("user"),
    'password': os.getenv("password"),
}
connection_str = (
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
    f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)
engine = create_engine(connection_str)
Session = sessionmaker(bind=engine)

tablas_bancos = [
    'bancamiga_bs_norm','bancamiga_us_norm','banesco99_bs_norm','banescopanama_us_norm',
    'banescoplanta_bs_norm','banescoverde_us_norm','banplus_bs_norm','bdv_bs_norm',
    'bnc_bs_norm','mercantil_bs_norm','mercantil_us_norm','mercantilpanama_us_norm',
    'provincial_bs_norm','provincial_us_norm'
]

def cargar_tabla_banco(banco):
    query = f"SELECT * FROM {banco}"
    return pd.read_sql(query, engine)

def crear_tabla_balance_general():
    tabla_balance_general = pd.DataFrame()
    for banco in tablas_bancos:
        df = cargar_tabla_banco(banco)
        tabla_balance_general = pd.concat([tabla_balance_general, df], ignore_index=True)
    return tabla_balance_general

if __name__ == "__main__":
    # 1) Construir DF base
    tabla_balance_general = crear_tabla_balance_general()

    if tabla_balance_general is None or tabla_balance_general.empty:
        raise RuntimeError("crear_tabla_balance_general() devolvió vacío o None.")

    print(f"DF base creado: {tabla_balance_general.shape} filas/columnas")

    # 2) Añadir columna inicial (opcional)
    if "clasificacion" not in tabla_balance_general.columns:
        tabla_balance_general["clasificacion"] = "Sin clasificacion"

    # paso previo ab clasificar usar los asientos para clasificar
    tabla_balance_general = searchenasientos(tabla_balance_general, engine)

    # 3) Clasificar usando Postgres (ILIKE por tokens)
    tabla_balance_general = Clasificarbalance(
        df=tabla_balance_general,
        engine=engine,
        table_name="ejmclasificaciones",
        umbral=0.75,           # Umbral inicial
        umbral_minimo=0.30,    # Hasta dónde bajar
        paso_umbral=0.05,      # Cuánto bajar cada vez
        verbose=True           # Ver progreso
    )

    # Verificación: debe ser DataFrame
    if not isinstance(tabla_balance_general, pd.DataFrame):
        raise TypeError("Clasificarbalance no devolvió un DataFrame.")

    print(f"DF clasificado: {tabla_balance_general.shape}")
    print(tabla_balance_general.head(5))

    # 4) crear ID unico
    #dropear columna id si existe
    if "id" in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.drop(columns=["id"])
    #crear ID unico
    tabla_balance_general["id"] = range(1, len(tabla_balance_general) + 1)

    # 5) Subir a Postgres
    try:
        tabla_balance_general.to_sql('balance_general', engine, if_exists='replace', index=False)
        print("Tabla balance_general cargada exitosamente")
    except Exception as e:
        print(f"Error al cargar la tabla balance_general: {e}")
