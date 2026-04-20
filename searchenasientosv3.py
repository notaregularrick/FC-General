import polars as pl
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def searchenasientos(df: pl.DataFrame, engine: Engine, mes: int = None, anio: int = None, verbose: bool = True) -> pl.DataFrame:
    """
    Busca coincidencias en la tabla 'asientos' usando referencia_bancaria.

    Si encuentra coincidencia:
    - Asigna 'Cliente/Proveedor' de asientos a 'proveedor_cliente' del df
      SOLO si proveedor_cliente está vacío o nulo.
    - Concatena 'centro_costo' a la descripción SOLO si no estaba ya presente.
    """

    if "referencia_bancaria" not in df.columns:
        if verbose:
            print("⚠️ Advertencia: No se encontró la columna 'referencia_bancaria' en el DataFrame")
        return df

    res = df.clone()

    if "proveedor_cliente" not in res.columns:
        res = res.with_columns(pl.lit(None).alias("proveedor_cliente"))

    if "clasificacion" not in res.columns:
        res = res.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))

    # Solo traer columnas necesarias
    query = """
        SELECT referencia, "Cliente/Proveedor", centro_costo
        FROM asientos
    """
    params = {}

    if mes is not None and anio is not None:
        query += """
            WHERE extract(month from fecha) = :mes
              AND extract(year from fecha) = :anio
        """
        params["mes"] = mes
        params["anio"] = anio

    try:
        with engine.connect() as conn:
            df_pandas = pd.read_sql(text(query), conn, params=params)
            asientos_df = pl.from_pandas(df_pandas)
        if verbose:
            print(f"✅ Cargados {asientos_df.height} registros de 'asientos'")
    except Exception as e:
        if verbose:
            print(f"❌ Error al cargar la tabla 'asientos': {e}")
        return df

    if asientos_df.is_empty():
        if verbose:
            print("⚠️ Advertencia: La tabla 'asientos' está vacía")
        return res

    # Limpiar y deduplicar por referencia para evitar multiplicar filas en el join
    asientos_df = (
        asientos_df
        .with_columns(
            pl.col("referencia").cast(pl.Utf8).str.strip_chars().alias("referencia"),
            pl.col("Cliente/Proveedor").cast(pl.Utf8).alias("Cliente/Proveedor"),
            pl.col("centro_costo").cast(pl.Utf8).alias("centro_costo"),
        )
        .filter(pl.col("referencia").is_not_null() & (pl.col("referencia") != ""))
        .group_by("referencia")
        .agg(
            pl.col("Cliente/Proveedor").drop_nulls().first().alias("__asiento_proveedor"),
            pl.col("centro_costo").drop_nulls().first().alias("__asiento_centro_costo"),
        )
    )

    if asientos_df.is_empty():
        if verbose:
            print("⚠️ No hay referencias válidas en 'asientos'")
        return res

    # Normalizar referencia del lado izquierdo también
    res = res.with_columns(
        pl.col("referencia_bancaria").cast(pl.Utf8).str.strip_chars().alias("referencia_bancaria")
    )

    merged = res.join(
        asientos_df,
        left_on="referencia_bancaria",
        right_on="referencia",
        how="left"
    )

    # Métricas correctas: match real cuando el join trajo algo del lado derecho
    matches_count = merged.filter(
        pl.col("__asiento_proveedor").is_not_null() | pl.col("__asiento_centro_costo").is_not_null()
    ).height

    proveedores_previos = merged.filter(
        pl.col("proveedor_cliente").is_not_null() & (pl.col("proveedor_cliente") != "")
    ).height

    # Solo completar proveedor_cliente si está vacío / nulo
    merged = merged.with_columns(
        pl.when(
            (pl.col("proveedor_cliente").is_null() | (pl.col("proveedor_cliente") == "")) &
            pl.col("__asiento_proveedor").is_not_null()
        )
        .then(pl.col("__asiento_proveedor"))
        .otherwise(pl.col("proveedor_cliente"))
        .alias("proveedor_cliente")
    )

    # Concatenar centro_costo solo si hay uno válido y no está ya en la descripción
    merged = merged.with_columns(
        pl.when(
            pl.col("__asiento_centro_costo").is_not_null() &
            ~pl.col("descripcion").cast(pl.Utf8).fill_null("").str.contains(
                pl.col("__asiento_centro_costo").cast(pl.Utf8),
                literal=True
            )
        )
        .then(
            pl.col("descripcion").cast(pl.Utf8).fill_null("") + pl.lit(" - ") + pl.col("__asiento_centro_costo")
        )
        .otherwise(pl.col("descripcion"))
        .alias("descripcion")
    )

    proveedores_post = merged.filter(
        pl.col("proveedor_cliente").is_not_null() & (pl.col("proveedor_cliente") != "")
    ).height

    proveedores_nuevos = max(proveedores_post - proveedores_previos, 0)

    merged = merged.drop(["__asiento_proveedor", "__asiento_centro_costo"])

    if verbose:
        print("📊 Resultados del match con 'asientos':")
        print(f" - Coincidencias reales encontradas: {matches_count}")
        print(f" - Proveedores/Clientes nuevos asignados: {proveedores_nuevos}")

    return merged
