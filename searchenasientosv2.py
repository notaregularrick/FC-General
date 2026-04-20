import polars as pl
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

def searchenasientos(df: pl.DataFrame, engine: Engine, mes: int = None, anio: int = None) -> pl.DataFrame:
    """
    Busca coincidencias en la tabla 'asientos' usando referencia_bancaria.
    
    Si encuentra coincidencia:
    - Asigna 'Cliente/Proveedor' de asientos a 'proveedor_cliente' del df
    - Concantena 'centro_costo' a la descripcion del df para mejorar la clasificacion
    """
    # Verificar que existe la columna referencia_bancaria
    if "referencia_bancaria" not in df.columns:
        print("⚠️  Advertencia: No se encontró la columna 'referencia_bancaria' en el DataFrame")
        return df
    
    # Cargar datos de la tabla asientos
    query = """
        SELECT tipo_operacion, fecha, referencia, "Cliente/Proveedor", centro_costo
        FROM asientos
    """
    if mes is not None and anio is not None:
        query = query + f" where extract(month from fecha) = {mes} and extract(year from fecha) = {anio}"
    
    try:
        with engine.connect() as conn:
            df_pandas = pd.read_sql(text(query), conn)
            asientos_df = pl.from_pandas(df_pandas)
        print(f"✅ Cargados {asientos_df.height} registros de la tabla 'asientos'")
    except Exception as e:
        print(f"❌ Error al cargar la tabla 'asientos': {e}")
        return df
    
    if asientos_df.is_empty():
        print("⚠️  Advertencia: La tabla 'asientos' está vacía")
        return df
    
    # Crear columna proveedor_cliente si no existe
    if "proveedor_cliente" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("proveedor_cliente"))
    
    # Asegurar que clasificacion existe (ya debería existir según el código principal)
    if "clasificacion" not in df.columns:
        df = df.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))
    
    # Hacer merge con referencia_bancaria = referencia
    # Usamos left join para mantener todas las filas del df original
    merged = df.join(
        asientos_df.select(["referencia", "Cliente/Proveedor", "centro_costo"]),
        left_on="referencia_bancaria",
        right_on="referencia",
        how="left"
    )
    
    # Asignar Cliente/Proveedor a proveedor_cliente donde haya match
    merged = merged.with_columns(
        pl.when(pl.col("Cliente/Proveedor").is_not_null())
        .then(pl.col("Cliente/Proveedor"))
        .otherwise(pl.col("proveedor_cliente"))
        .alias("proveedor_cliente")
    )
    
    # Concantenar centro_costo a la descripcion del df para mejorar la clasificacion
    merged = merged.with_columns(
        pl.when(pl.col("centro_costo").is_not_null())
        .then(pl.col("descripcion") + " - " + pl.col("centro_costo"))
        .otherwise(pl.col("descripcion"))
        .alias("descripcion")
    )
    
    # Eliminar columnas temporales del merge (solo si existen)
    cols_a_eliminar = ["referencia", "Cliente/Proveedor", "centro_costo"]
    cols_existentes = [col for col in cols_a_eliminar if col in merged.columns]
    if cols_existentes:
        merged = merged.drop(cols_existentes)
    
    # Contar cuántos registros se actualizaron
    matches_count = merged.select(pl.col("referencia_bancaria").is_not_null()).sum().item()
    proveedores_actualizados = merged.select(pl.col("proveedor_cliente").is_not_null()).sum().item()
    
    print(f"📊 Resultados del match con 'asientos':")
    print(f"   - Coincidencias encontradas: {matches_count}")
    print(f"   - Proveedores/Clientes asignados: {proveedores_actualizados}")
    
    return merged