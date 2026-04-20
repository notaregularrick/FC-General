import polars as pl
import pandas as pd
import unicodedata
import re
from sqlalchemy.engine import Engine
from sqlalchemy import text


def _normalizar(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def asignar_prov_cli(df: pl.DataFrame, engine: Engine, verbose: bool = True) -> pl.DataFrame:
    """
    Asigna proveedor_cliente a cada fila del DataFrame buscando coincidencias
    del nombre o rif de cliente_proveedores dentro de la descripcion.
    """
    with engine.connect() as conn:
        df_pandas = pd.read_sql(text("SELECT nombre, rif FROM cliente_proveedores"), conn)
        proveedores = pl.from_pandas(df_pandas)

    if proveedores.is_empty():
        if verbose:
            print("⚠️  La tabla cliente_proveedores está vacía.")
        return df

    proveedores = proveedores.drop_nulls("nombre")
    proveedores = proveedores.with_columns(
        pl.col("nombre").map_elements(_normalizar, return_dtype=pl.Utf8).alias("nombre_norm"),
        pl.col("rif").fill_null("").map_elements(_normalizar, return_dtype=pl.Utf8).alias("rif_norm")
    )

    # Filtrar entradas con nombre normalizado vacío o muy corto (< 3 chars)
    proveedores = proveedores.filter(pl.col("nombre_norm").str.len_chars() >= 3)

    nombres_norm = proveedores["nombre_norm"].to_list()
    rifs_norm = proveedores["rif_norm"].to_list()
    nombres_orig = proveedores["nombre"].to_list()

    res = df.clone()
    if "proveedor_cliente" not in res.columns:
        res = res.with_columns(pl.lit("").alias("proveedor_cliente"))

    desc_norm = res["descripcion"].cast(pl.Utf8).map_elements(_normalizar, return_dtype=pl.Utf8)

    asignados = 0
    total = len(res)

    for i in range(total):
        if res["proveedor_cliente"][i] and res["proveedor_cliente"][i] != "":
            continue

        desc = desc_norm[i]
        if not desc:
            continue

        for j in range(len(nombres_norm)):
            # Buscar por RIF primero (es identificador único, más confiable)
            if rifs_norm[j] and len(rifs_norm[j]) >= 5 and rifs_norm[j] in desc:
                res = res.with_columns(
                    pl.when(pl.arange(0, pl.len()) == i)
                    .then(pl.lit(nombres_orig[j]))
                    .otherwise(pl.col("proveedor_cliente"))
                    .alias("proveedor_cliente")
                )
                asignados += 1
                break
            # Buscar por nombre
            if nombres_norm[j] in desc:
                res = res.with_columns(
                    pl.when(pl.arange(0, pl.len()) == i)
                    .then(pl.lit(nombres_orig[j]))
                    .otherwise(pl.col("proveedor_cliente"))
                    .alias("proveedor_cliente")
                )
                asignados += 1
                break

        if verbose and ((i + 1) % 500 == 0 or (i + 1) == total):
            print(f"   · Progreso: {i + 1}/{total} procesados (asignados: {asignados})")

    if verbose:
        print(f"✅ Asignación completada: {asignados} de {total} filas con proveedor_cliente asignado.")

    return res
