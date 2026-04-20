import os
import pandas as pd
import polars as pl

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from searchenasientosv3 import searchenasientos
from Clasificador_nuevov3 import Clasificarbalance
from clasificar_balance_historicov3 import clasificar_con_balance_historico
from asignar_prov_cliv2 import asignar_prov_cli

# =========================================================
# CONFIG
# =========================================================

load_dotenv()

db_config = {
    "host": os.getenv("host"),
    "port": os.getenv("port"),
    "database": os.getenv("database"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
}

connection_str = (
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
    f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

engine = create_engine(connection_str)
Session = sessionmaker(bind=engine)

tablas_bancos = [
    "bancamiga_bs_norm","bancamiga_us_norm","banesco99_bs_norm","banescopanama_us_norm",
    "banescoplanta_bs_norm","banescoverde_us_norm","banplus_bs_norm","bdv_bs_norm",
    "bnc_bs_norm","bnc_us_norm","mercantil_bs_norm","mercantil_us_norm","mercantilpanama_us_norm",
    "provincial_bs_norm","provincial_us_norm","bvc_bs_norm","caja_us_norm","bnc6550_bs_norm"
]

# =========================================================
# Carga de datos
# =========================================================

def cargar_tabla_banco(banco: str, anio: int, mes: int) -> pl.DataFrame:
    query = f"""
        SELECT *
        FROM {banco}
        WHERE extract(year from fecha) = {anio}
          AND extract(month from fecha) = {mes}
    """
    with engine.connect() as conn:
        df_pandas = pd.read_sql(text(query), conn)
    return pl.from_pandas(df_pandas)

def crear_tabla_balance_general(anio: int, mes: int) -> pl.DataFrame:
    dfs = []
    for banco in tablas_bancos:
        df = cargar_tabla_banco(banco, anio, mes)
        if df is not None and not df.is_empty():
            dfs.append(df)

    if not dfs:
        return pl.DataFrame()

    return pl.concat(dfs, how="diagonal_relaxed")

# =========================================================
# Reglas rápidas previas / posteriores
# =========================================================

def aplicar_reglas_directas(df: pl.DataFrame) -> pl.DataFrame:
    if "clasificacion" not in df.columns:
        df = df.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))

    desc = pl.col("descripcion").cast(pl.Utf8).fill_null("")

    df = df.with_columns(
        pl.when(desc.str.contains("TRASPASO ENTRE CUENTAS", literal=True) | 
                desc.str.contains("TRANSFERENCIA ENTRE CUENTAS", literal=True) |
                desc.str.contains("transferencia entre cuentas", literal=True) 
        )
        .then(pl.lit("Transferencia entre cuentas"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )

    df = df.with_columns(
        pl.when(
            (pl.col("clasificacion") == "Sin clasificacion") & (
            desc.str.contains("Pago de cliente", literal=True) |
            desc.str.contains("Cobranza", literal=True)
            )
        )
        .then(pl.lit("Cobranzas"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )

    df = df.with_columns(
        pl.when(
            (pl.col("clasificacion") == "Sin clasificacion") &
            desc.str.contains("Pago de proveedor", literal=True)
        )
        .then(pl.lit("Otros Gastos"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )

    df = df.with_columns(
        pl.when(
            (pl.col("clasificacion") == "Sin clasificacion") & (
            desc.str.contains("comisiones financieras", literal=True) |
            desc.str.contains("COMISIONES FINANCIERAS", literal=True) 
            )
        )
        .then(pl.lit("Comisiones"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )

    df = df.with_columns(
        pl.when(
            (pl.col("clasificacion") == "Sin clasificacion") & (
            desc.str.contains("CANTV", literal=True)
            | desc.str.contains("HIDROCAPITAL", literal=True)
            | desc.str.contains("CORPOELEC", literal=True))
        )
        .then(pl.lit("Servicios"))
        .otherwise(pl.col("clasificacion"))
        .alias("clasificacion")
    )

    return df

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    mes_a_cargar = 3
    anio_a_cargar = 2026

    print(f"🚀 Iniciando carga de balance_general para {anio_a_cargar}-{mes_a_cargar:02d}...")

    tabla_balance_general = crear_tabla_balance_general(anio_a_cargar, mes_a_cargar)

    if tabla_balance_general is None or tabla_balance_general.is_empty():
        raise RuntimeError("crear_tabla_balance_general() devolvió vacío o None.")

    print(f"✅ DF base creado: {tabla_balance_general.shape} (filas, columnas)")

    # columnas base
    if "clasificacion" not in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.with_columns(
            pl.lit("Sin clasificacion").alias("clasificacion")
        )

    if "clasificacion_global" not in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.with_columns(
            pl.lit("").alias("clasificacion_global")
        )

    if "score_clasificador" not in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.with_columns(
            pl.lit(0.0).alias("score_clasificador")
        )

    if "proveedor_cliente" not in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.with_columns(
            pl.lit(None).alias("proveedor_cliente")
        )


    # Limpiar columna de referencia: quitar caracter "'"
    tabla_balance_general = tabla_balance_general.with_columns(
        pl.col("referencia_bancaria").str.replace("'", "").alias("referencia_bancaria")
    )


    # -----------------------------------------------------
    # 1) clasificación con asientos
    # -----------------------------------------------------
    print("📒 Ejecutando clasificación por asientos...")
    tabla_balance_general = searchenasientos(
        tabla_balance_general,
        engine,
        mes_a_cargar,
        anio_a_cargar
    )

    # -----------------------------------------------------
    # 2) reglas directas
    # -----------------------------------------------------
    print("⚡ Aplicando reglas directas...")
    tabla_balance_general = aplicar_reglas_directas(tabla_balance_general)

    print(
        f" - Transferencia entre cuentas: "
        f"{tabla_balance_general.filter(pl.col('clasificacion') == 'Transferencia entre cuentas').height}"
    )
    print(
        f" - Cobranzas: "
        f"{tabla_balance_general.filter(pl.col('clasificacion') == 'Cobranzas').height}"
    )
    print(
        f" - Comisiones: "
        f"{tabla_balance_general.filter(pl.col('clasificacion') == 'Comisiones').height}"
    )
    print(        f" - Servicios: "
        f"{tabla_balance_general.filter(pl.col('clasificacion') == 'Servicios').height}"
    )
    print(
        f" - Otros Gastos: "
        f"{tabla_balance_general.filter(pl.col('clasificacion') == 'Otros Gastos').height}"
    )

    # -----------------------------------------------------
    # 3) clasificador principal (catálogo)
    # -----------------------------------------------------
    print("⚙️ Iniciando clasificación principal con catálogo...")
    tabla_balance_general = Clasificarbalance(
        df=tabla_balance_general,
        engine=engine,
        table_name="ejmclasificaciones",
        umbral=0.90,
        umbral_minimo=0.50,
        paso_umbral=0.05,
        max_iteraciones=20,
        verbose=True
    )

    sin_clasificar_post_principal = tabla_balance_general.filter(
        pl.col("clasificacion") == "Sin clasificacion"
    ).height

    print(
        f"📌 Tras clasificador principal: "
        f"{sin_clasificar_post_principal} registros siguen 'Sin clasificacion'"
    )

    # -----------------------------------------------------
    # 4) clasificador histórico
    # -----------------------------------------------------
    print("📚 Iniciando clasificación histórica con balance_general...")
    umbral_historico = 0.80
    # solo clasificar con histórico si hay registros sin clasificar
    df_sin_clasificar = tabla_balance_general.filter(pl.col("clasificacion") == "Sin clasificacion")
    # eliminar los conceptos ya clasificados del general para que no interfieran en el histórico
    tabla_balance_general = tabla_balance_general.filter(pl.col("clasificacion") != "Sin clasificacion")
    while umbral_historico >= 0.50 and df_sin_clasificar.filter(pl.col("clasificacion") == "Sin clasificacion").height > 0:
        df_sin_clasificar = clasificar_con_balance_historico(
            df_sin_clasificar,
            engine,
            umbral_similitud=umbral_historico,
            verbose=True
        )
        print(f"Se usó umbral histórico: {umbral_historico:.2f}")
        umbral_historico = round(umbral_historico - 0.10, 2)

    print(
        f"📌 Tras clasificación histórica: "
        f"{df_sin_clasificar.filter(pl.col('clasificacion') == 'Sin clasificacion').height} "
        f"registros siguen 'Sin clasificacion'"
    )

    # volver a unir el histórico clasificado con el general
    tabla_balance_general = pl.concat([tabla_balance_general, df_sin_clasificar], how="diagonal_relaxed")


    # -----------------------------------------------------
    # 5) asignar proveedor_cliente
    # -----------------------------------------------------
    print("🏷️ Iniciando asignación de proveedor_cliente...")
    tabla_balance_general = asignar_prov_cli(tabla_balance_general, engine)

    if not isinstance(tabla_balance_general, pl.DataFrame):
        raise TypeError("El pipeline no devolvió un DataFrame Polars.")

    print(f"DF clasificado: {tabla_balance_general.shape}")
    print(tabla_balance_general.head(5))
    print(tabla_balance_general.columns)

    # -----------------------------------------------------
    # 6) ID único
    # -----------------------------------------------------
    if "id" in tabla_balance_general.columns:
        tabla_balance_general = tabla_balance_general.drop("id")

    tabla_balance_general = tabla_balance_general.with_columns(
        pl.arange(100000, 100000 + tabla_balance_general.height).alias("id")
    )

    # -----------------------------------------------------
    # 7) limpieza final
    # -----------------------------------------------------
    cols_to_drop = [c for c in ["tasa_de_cambio", "tipo_de_operacion"] if c in tabla_balance_general.columns]
    if cols_to_drop:
        tabla_balance_general = tabla_balance_general.drop(cols_to_drop)
        print(f"Dropeadas columnas: {cols_to_drop}")

    print(f"DF final listo para subir a Postgres: {tabla_balance_general.shape}")
    print(
        "Registros sin clasificar tras todo el proceso:",
        tabla_balance_general.filter(pl.col("clasificacion") == "Sin clasificacion").height
    )

    # -----------------------------------------------------
    # 8) subir a Postgres
    # -----------------------------------------------------
    try:
        tabla_balance_general.to_pandas().to_sql(
            "balance_general",
            engine,
            if_exists="append",
            index=False
        )
        print("✅ Tabla balance_general cargada exitosamente")
    except Exception as e:
        print(f"❌ Error al cargar la tabla balance_general: {e}")