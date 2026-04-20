import polars as pl
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
import unicodedata
import re
from rapidfuzz import process, fuzz
from typing import List, Dict, Set
from collections import defaultdict

# -------------------------------------------
# Funciones de similitud de texto (reutilizadas del Clasificador)
# -------------------------------------------
_STOPWORDS_ES = {
    "de","la","el","y","en","por","para","con","a","del","los","las","un","una","al",
    "o","u","que","se","su","sus","otros","otra","otro","otra","sobre","le","les",
    "es","son","fue","ser","esta","este","esto","está","más","menos","muy",
    "me","mi","mis","tu","tus"
}

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def _normalize_text(s: str) -> str:
    s = _strip_accents(str(s).lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(s: str) -> List[str]:
    if not s:
        return []
    return [t for t in s.split(" ") if t and t not in _STOPWORDS_ES]

def _jaccard(tokens_a: List[str], tokens_b: List[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    a, b = set(tokens_a), set(tokens_b)
    return len(a & b) / len(a | b) if (a or b) else 0.0

def _seq_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def _score_similitud(a_norm: str, b_norm: str, a_toks: List[str], b_toks: List[str]) -> float:
    # Usar RapidFuzz ratio
    return fuzz.ratio(a_norm, b_norm) / 100

# -------------------------------------------
# Clasificación previa usando balance_general histórico
# -------------------------------------------
def clasificar_con_balance_historico(df: pl.DataFrame, engine: Engine, umbral_similitud: float = 0.85) -> pl.DataFrame:
    """
    Busca coincidencias en la tabla 'balance_general' usando similitud de descripción.
    
    Si encuentra coincidencia alta:
    - Asigna 'clasificacion' de balance_general al df
    - Asigna 'proveedor_cliente' de balance_general al df
    
    Parámetros:
    - df: DataFrame con columna 'descripcion'
    - engine: Engine de SQLAlchemy conectado a Postgres
    - umbral_similitud: Umbral mínimo de similitud para considerar coincidencia (default 0.85)
    
    Devuelve el DataFrame con clasificaciones y proveedores asignados donde haya coincidencias.
    """
    # Verificar que existe la columna descripcion
    if "descripcion" not in df.columns:
        print("⚠️  Advertencia: No se encontró la columna 'descripcion' en el DataFrame")
        return df
    
    # Cargar datos históricos de balance_general
    query = """
        SELECT fecha, mes, semana, referencia_bancaria, descripcion, 
               monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, 
               concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, 
               es_saldo_final, saldo_final_calculado, clasificacion, clasificacion_global, 
               score_clasificador, id
        FROM balance_general
    """
    
    try:
        with engine.connect() as conn:
            df_pandas = pd.read_sql(text(query), conn)
            balance_historico_df = pl.from_pandas(df_pandas)
        print(f"✅ Cargados {balance_historico_df.height} registros históricos de 'balance_general'")
    except Exception as e:
        print(f"❌ Error al cargar la tabla 'balance_general': {e}")
        return df
    
    if balance_historico_df.is_empty():
        print("⚠️  Advertencia: La tabla 'balance_general' está vacía")
        return df
    
    # Crear columnas si no existen
    if "proveedor_cliente" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("proveedor_cliente"))
    
    if "clasificacion" not in df.columns:
        df = df.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))
    
    # Normalizar descripciones del histórico
    print("🧮 Normalizando y tokenizando descripciones históricas...")
    balance_historico_df = balance_historico_df.with_columns(
        pl.col("descripcion").cast(pl.Utf8).map_elements(_normalize_text, return_dtype=pl.Utf8).alias("desc_norm"),
        pl.col("descripcion").cast(pl.Utf8).map_elements(_tokenize, return_dtype=pl.List(pl.Utf8)).alias("desc_tokens")
    )
    
    # Filtrar solo registros con clasificación válida
    balance_historico_df = balance_historico_df.filter(
        (pl.col("clasificacion").is_not_null()) & 
        (pl.col("clasificacion") != "Sin clasificacion")
    )
    
    if balance_historico_df.is_empty():
        print("⚠️  Advertencia: No hay registros históricos con clasificación válida")
        return df

    # ----------------------------------------------------
    # PRE-CÁLCULO: normalizar y tokenizar texto actual una sola vez
    # ----------------------------------------------------
    print("🧮 Normalizando y tokenizando descripciones actuales...")
    df = df.with_columns(
        pl.col("descripcion").cast(pl.Utf8).map_elements(_normalize_text, return_dtype=pl.Utf8).alias("desc_norm"),
        pl.col("descripcion").cast(pl.Utf8).map_elements(_tokenize, return_dtype=pl.List(pl.Utf8)).alias("desc_tokens")
    )

    longitudes_tokens = df["desc_tokens"].map_elements(len, return_dtype=pl.Int32)
    print(
        f"   - Registros actuales con tokens vacíos: "
        f"{longitudes_tokens.filter(longitudes_tokens == 0).len()} de {df.height}"
    )
    if longitudes_tokens.filter(longitudes_tokens > 0).len() > 0:
        print(
            f"   - Tokens promedio por registro (solo >0): "
            f"{longitudes_tokens.filter(longitudes_tokens > 0).mean():.2f}"
        )

    # ----------------------------------------------------
    # OPTIMIZACIÓN: índices para evitar comparar con todo
    # ----------------------------------------------------
    # 1) Mapa de coincidencias exactas por descripción normalizada
    mapa_exact: Dict[str, int] = {}
    # convertir a listas de Python para iterar con índices
    hist_descs = balance_historico_df["desc_norm"].to_list()
    for hist_idx, desc_norm_hist in enumerate(hist_descs):
        if desc_norm_hist and desc_norm_hist not in mapa_exact:
            mapa_exact[desc_norm_hist] = hist_idx

    # 2) Índice invertido token -> conjunto de índices candidatos
    indice_invertido: Dict[str, Set[int]] = defaultdict(set)
    hist_tokens_list = balance_historico_df["desc_tokens"].to_list()
    for hist_idx, tokens in enumerate(hist_tokens_list):
        if not tokens:
            continue
        for t in tokens:
            # ignorar tokens muy cortos (ruido)
            if len(t) >= 3:
                indice_invertido[t].add(hist_idx)

    # Convertir df actual a pandas para iterar y actualizar fácilmente
    df_pd = df.to_pandas()
    hist_pd = balance_historico_df.to_pandas()

    # Procesar solo filas sin clasificar
    clasificaciones_asignadas = 0
    proveedores_asignados = 0

    indices_pendientes = df_pd.index[df_pd["clasificacion"] == "Sin clasificacion"].tolist()

    total_pendientes = len(indices_pendientes)
    print(f"🔎 Inicio de clasificación histórica. Pendientes: {total_pendientes}")

    procesados = 0

    for pos, idx in enumerate(indices_pendientes, start=1):
        desc_norm_actual = df_pd.at[idx, "desc_norm"]
        desc_tokens_actual = df_pd.at[idx, "desc_tokens"]

        if not desc_norm_actual:
            continue

        # 1) Intento rápido: coincidencia exacta por descripción normalizada
        hist_idx_exact = mapa_exact.get(desc_norm_actual)
        if hist_idx_exact is not None:
            df_pd.at[idx, "clasificacion"] = hist_pd.at[hist_idx_exact, "clasificacion"]

            proveedor = hist_pd.at[hist_idx_exact, "proveedor_cliente"]
            if pd.notna(proveedor):
                df_pd.at[idx, "proveedor_cliente"] = proveedor
                proveedores_asignados += 1

            clasificaciones_asignadas += 1
            continue

        # 2) Búsqueda aproximada: limitar candidatos usando tokens
        # desc_tokens_actual puede ser lista, tuple o ndarray; tratarlo como secuencia
        if desc_tokens_actual is None:
            continue
        if hasattr(desc_tokens_actual, "__len__") and len(desc_tokens_actual) == 0:
            continue

        candidatos: Set[int] = set()
        for t in desc_tokens_actual:
            if len(t) >= 3 and t in indice_invertido:
                candidatos.update(indice_invertido[t])

        if not candidatos:
            continue

        mejor_score = -1.0
        mejor_idx = -1

        for hist_idx in candidatos:
            desc_norm_hist = hist_pd.at[hist_idx, "desc_norm"]
            desc_tokens_hist = hist_pd.at[hist_idx, "desc_tokens"]

            score = _score_similitud(
                desc_norm_actual, desc_norm_hist,
                desc_tokens_actual, desc_tokens_hist
            )

            if score > mejor_score:
                mejor_score = score
                mejor_idx = hist_idx

        # Si la similitud es alta, asignar clasificación y proveedor
        if mejor_score >= umbral_similitud and mejor_idx != -1:
            df_pd.at[idx, "clasificacion"] = hist_pd.at[mejor_idx, "clasificacion"]

            proveedor = hist_pd.at[mejor_idx, "proveedor_cliente"]
            if pd.notna(proveedor):
                df_pd.at[idx, "proveedor_cliente"] = proveedor
                proveedores_asignados += 1

            clasificaciones_asignadas += 1

        procesados += 1

        # Log de progreso cada 1000 registros pendientes procesados
        if pos % 1000 == 0 or pos == total_pendientes:
            print(
                f"      · Progreso histórico: {pos}/{total_pendientes} procesados "
                f"(clasificaciones asignadas: {clasificaciones_asignadas}, "
                f"proveedores asignados: {proveedores_asignados})"
            )

    print(f"📊 Resultados de clasificación con balance histórico:")
    print(f"   - Clasificaciones asignadas: {clasificaciones_asignadas}")
    print(f"   - Proveedores/Clientes asignados: {proveedores_asignados}")
    print(f"   - Umbral de similitud usado: {umbral_similitud}")

    # Eliminar columnas auxiliares internas antes de devolver (ya estamos en pandas)
    df_pd = df_pd.drop(columns=["desc_norm", "desc_tokens"])

    # Convertir de nuevo a Polars para que el resto del pipeline sea consistente
    return pl.from_pandas(df_pd)

