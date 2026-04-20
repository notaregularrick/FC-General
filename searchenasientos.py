import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def searchenasientos(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """
    Busca coincidencias en la tabla 'asientos' usando referencia_bancaria.
    
    Si encuentra coincidencia:
    - Asigna 'Cliente/Proveedor' de asientos a 'proveedor_cliente' del df
    - Asigna 'centro_costo' de asientos a 'clasificacion' del df (solo si no es nulo)
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
    
    try:
        asientos_df = pd.read_sql(query, engine)
        print(f"✅ Cargados {len(asientos_df)} registros de la tabla 'asientos'")
    except Exception as e:
        print(f"❌ Error al cargar la tabla 'asientos': {e}")
        return df
    
    if asientos_df.empty:
        print("⚠️  Advertencia: La tabla 'asientos' está vacía")
        return df
    
    # Crear columna proveedor_cliente si no existe
    if "proveedor_cliente" not in df.columns:
        df["proveedor_cliente"] = None
    
    # Asegurar que clasificacion existe (ya debería existir según el código principal)
    if "clasificacion" not in df.columns:
        df["clasificacion"] = "Sin clasificacion"
    
    # Hacer merge con referencia_bancaria = referencia
    # Usamos left join para mantener todas las filas del df original
    merged = df.merge(
        asientos_df[["referencia", "Cliente/Proveedor", "centro_costo"]],
        left_on="referencia_bancaria",
        right_on="referencia",
        how="left",
        suffixes=("", "_asientos")
    )
    
    # Asignar Cliente/Proveedor a proveedor_cliente donde haya match
    mask_match = merged["referencia"].notna()
    merged.loc[mask_match, "proveedor_cliente"] = merged.loc[mask_match, "Cliente/Proveedor"]
    
    # Asignar centro_costo a clasificacion solo si no es nulo
    mask_centro_costo = mask_match & merged["centro_costo"].notna()
    merged.loc[mask_centro_costo, "clasificacion"] = merged.loc[mask_centro_costo, "centro_costo"]
    
    # Eliminar columnas temporales del merge
    merged = merged.drop(columns=["referencia", "Cliente/Proveedor", "centro_costo"], errors="ignore")
    
    # Contar cuántos registros se actualizaron
    matches_count = mask_match.sum()
    clasificaciones_actualizadas = mask_centro_costo.sum()
    proveedores_actualizados = mask_match.sum()
    
    print(f"📊 Resultados del match con 'asientos':")
    print(f"   - Coincidencias encontradas: {matches_count}")
    print(f"   - Proveedores/Clientes asignados: {proveedores_actualizados}")
    print(f"   - Clasificaciones actualizadas desde centro_costo: {clasificaciones_actualizadas}")
    
    return merged