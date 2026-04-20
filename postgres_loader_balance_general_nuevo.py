
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

from Clasificador_nuevo import Clasificarbalance
from clasificar_balance_historico import clasificar_con_balance_historico
from asignar_prov_cli import asignar_prov_cli


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

def cargar_tabla_banco(banco,anio,mes):
    query = f"SELECT * FROM {banco}"
    query = query + f" where extract(year from fecha) = {anio}"
    query = query + f" and extract(month from fecha) = {mes}"

    return pd.read_sql(query, engine)

def crear_tabla_balance_general(anio,mes):
    tabla_balance_general = pd.DataFrame()
    for banco in tablas_bancos:
        df = cargar_tabla_banco(banco,anio,mes)
        tabla_balance_general = pd.concat([tabla_balance_general, df], ignore_index=True)
    return tabla_balance_general

if __name__ == "__main__":

    mes_a_cargar = 1
    anio_a_cargar = 2026

    # 1) Construir DF base
    print(f"🚀 Iniciando carga de balance_general para {anio_a_cargar}-{mes_a_cargar:02d}...")
    tabla_balance_general = crear_tabla_balance_general(anio_a_cargar,mes_a_cargar)

    if tabla_balance_general is None or tabla_balance_general.empty:
        raise RuntimeError("crear_tabla_balance_general() devolvió vacío o None.")

    print(f"✅ DF base creado: {tabla_balance_general.shape} (filas, columnas)")

    # 2) Añadir columna inicial (opcional)
    if "clasificacion" not in tabla_balance_general.columns:
        tabla_balance_general["clasificacion"] = "Sin clasificacion"

    # 3) Clasificar usando Postgres (ILIKE por tokens)
    print("⚙️  Iniciando clasificación principal con catálogo Postgres...")
    tabla_balance_general = Clasificarbalance(
        df=tabla_balance_general,
        engine=engine,
        table_name="ejmclasificaciones",
        umbral=0.80,           # Umbral inicial
        umbral_minimo=0.20,    # Hasta dónde bajar
        paso_umbral=0.05,      # Cuánto bajar cada vez
        verbose=True           # Ver progreso
    )

    sin_clasificar_post_principal = (tabla_balance_general["clasificacion"] == "Sin clasificacion").sum()
    print(
        f"📌 Tras clasificador principal: "
        f"{sin_clasificar_post_principal} registros siguen 'Sin clasificacion'"
    )

    # 4) paso posterior a clasificar, clasificar con movimientos previos en balance_general
    print("📚 Iniciando clasificación basada en histórico de 'balance_general'...")
    tabla_balance_general = clasificar_con_balance_historico(
        tabla_balance_general,
        engine,
        umbral_similitud=0.40
    )

    # 5) asignar proveedor_cliente
    print("📚 Iniciando asignación de proveedor_cliente...")
    tabla_balance_general = asignar_prov_cli(tabla_balance_general, engine)

    # Verificación: debe ser DataFrame
    if not isinstance(tabla_balance_general, pd.DataFrame):
        raise TypeError("Clasificarbalance no devolvió un DataFrame.")

    print(f"DF clasificado: {tabla_balance_general.shape}")
    print(tabla_balance_general.head(5))

    # 4) crear ID unico
    #dropear columna id si existe
    if not "id" in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.drop(columns=["id"])
        #crear ID unico
        tabla_balance_general["id"] = range(100000, len(tabla_balance_general) + 1)

    # 5) Subir a Postgres
    try:
        tabla_balance_general.to_sql('balance_general', engine, if_exists='append', index=False)
        print("Tabla balance_general cargada exitosamente")
    except Exception as e:
        print(f"Error al cargar la tabla balance_general: {e}")
