
import polars as pl
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
from searchenasientosv2 import searchenasientos
from Clasificador_nuevov2 import Clasificarbalance
from clasificar_balance_historicov2 import clasificar_con_balance_historico
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
    'bnc_bs_norm','bnc_us_norm','mercantil_bs_norm','mercantil_us_norm','mercantilpanama_us_norm',
    'provincial_bs_norm','provincial_us_norm','bvc_bs_norm','caja_us_norm'
]

def cargar_tabla_banco(banco,anio,mes):
    query = f"SELECT * FROM {banco}"
    query = query + f" where extract(year from fecha) = {anio}"
    query = query + f" and extract(month from fecha) = {mes}"

    with engine.connect() as conn:
        df_pandas = pd.read_sql(text(query), conn)
        return pl.from_pandas(df_pandas)

def crear_tabla_balance_general(anio,mes):
    dfs = []
    for banco in tablas_bancos:
        df = cargar_tabla_banco(banco,anio,mes)
        dfs.append(df)
    return pl.concat(dfs, how="diagonal_relaxed")

if __name__ == "__main__":

    mes_a_cargar = 3
    anio_a_cargar = 2026

    # 1) Construir DF base
    print(f"🚀 Iniciando carga de balance_general para {anio_a_cargar}-{mes_a_cargar:02d}...")
    tabla_balance_general = crear_tabla_balance_general(anio_a_cargar,mes_a_cargar)

    if tabla_balance_general is None or tabla_balance_general.is_empty():
        raise RuntimeError("crear_tabla_balance_general() devolvió vacío o None.")

    print(f"✅ DF base creado: {tabla_balance_general.shape} (filas, columnas)")

    # 2) Añadir columna inicial (opcional)
    if "clasificacion" not in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))

    # paso previo antes de clasificar usar los asientos para clasificar
    tabla_balance_general = searchenasientos(tabla_balance_general, engine, mes_a_cargar, anio_a_cargar)

    # si la descripcion contiene "TRASPASO ENTRE CUENTAS" de la empresa, clasificar como "Transferencia entre cuentas"
    tabla_balance_general = tabla_balance_general.with_columns(
        pl.when(
            (pl.col("descripcion").str.contains("TRASPASO ENTRE CUENTAS")) 
        )
        .then(pl.lit("Transferencia entre cuentas"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )

    print(f"Se han clasificado {tabla_balance_general.filter(pl.col('clasificacion') == 'Transferencia entre cuentas').height} registros como 'Transferencia entre cuentas'")

    # antes del clasificador principal, si en descripcion hay "Pago de cliente", clasificar como "Cobranzas"
    tabla_balance_general = tabla_balance_general.with_columns(
        pl.when((pl.col("clasificacion") == "Sin clasificacion") & pl.col("descripcion").str.contains("Pago de cliente"))
        .then(pl.lit("Cobranzas"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )

    print(f"Se han clasificado {tabla_balance_general.filter(pl.col('clasificacion') == 'Cobranzas').height} registros como 'Cobranzas'")



    # 3) Clasificar usando Postgres (ILIKE por tokens)
    print("⚙️  Iniciando clasificación principal con catálogo Postgres...")
    tabla_balance_general = Clasificarbalance(
        df=tabla_balance_general,
        engine=engine,
        table_name="ejmclasificaciones",
        umbral=0.80,           # Umbral inicial
        umbral_minimo=0.30,    # Hasta dónde bajar
        paso_umbral=0.1,      # Cuánto bajar cada vez
        verbose=True           # Ver progreso
    )

    # Si despues de clasificar hay "sin clasificacion" y en descripcion hay "Pago de proveedor", clasificar como "Otros Gastos"
    tabla_balance_general = tabla_balance_general.with_columns(
        pl.when((pl.col("clasificacion") == "Sin clasificacion") & pl.col("descripcion").str.contains("Pago de proveedor"))
        .then(pl.lit("Otros Gastos"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )
    print(f"Se han clasificado {tabla_balance_general.filter(pl.col('clasificacion') == 'Otros Gastos').height} registros como 'Otros Gastos'")

    sin_clasificar_post_principal = tabla_balance_general.filter(pl.col("clasificacion") == "Sin clasificacion").height
    print(
        f"📌 Tras clasificador principal: "
        f"{sin_clasificar_post_principal} registros siguen 'Sin clasificacion'"
    )

    # 4) paso posterior a clasificar, clasificar con movimientos previos en balance_general
    print("📚 Iniciando clasificación basada en histórico de 'balance_general'...")

    UmbralHistorico = 0.80  # Umbral inicial para clasificación histórica

    while UmbralHistorico >= 0.30:  # Repetir varias veces para captar más registros similares
        tabla_balance_general = clasificar_con_balance_historico(
            tabla_balance_general,
            engine,
            umbral_similitud=UmbralHistorico
        )
        print('Se uso el umbral de similitud historico hasta:', UmbralHistorico + 0.10)
        UmbralHistorico -= 0.10  # Reducir el umbral para captar más registros en cada iteración
    
    print(f"📌 Tras clasificación histórica: {tabla_balance_general.filter(pl.col('clasificacion') == 'Sin clasificacion').height} registros siguen 'Sin clasificacion'")

    # 5) asignar proveedor_cliente
    print("📚 Iniciando asignación de proveedor_cliente...")
    tabla_balance_general = asignar_prov_cli(tabla_balance_general, engine)

    # Verificación: debe ser DataFrame
    if not isinstance(tabla_balance_general, pl.DataFrame):
        raise TypeError("Clasificarbalance no devolvió un DataFrame.")

    print(f"DF clasificado: {tabla_balance_general.shape}")
    print(tabla_balance_general.head(5))
    print(tabla_balance_general.columns)

    # 4) crear ID unico
    #dropear columna id si existe
    if "id" in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.drop("id")
    #crear ID unico
    tabla_balance_general = tabla_balance_general.with_columns(
        pl.arange(100000, 100000 + tabla_balance_general.height).alias("id")
    )
    # dropear columna 
    if "tasa_de_cambio" in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.drop(["tasa_de_cambio", "tipo_de_operacion"])
        print("Dropeada columna tasa de cambio duplicada")

    print(f"DF final listo para subir a Postgres: {tabla_balance_general.shape}")
    print("Registros sin clasificar tras todo el proceso:", tabla_balance_general.filter(pl.col("clasificacion") == "Sin clasificacion").height)

    # 5) Subir a Postgres
    try:
        # Convertir a pandas para to_sql
        tabla_balance_general.to_pandas().to_sql('balance_general', engine, if_exists='append', index=False)
        print("Tabla balance_general cargada exitosamente")
    except Exception as e:
        print(f"Error al cargar la tabla balance_general: {e}")
