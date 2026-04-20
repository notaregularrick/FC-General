# Clasificador.py
from difflib import SequenceMatcher

import polars as pl
import pandas as pd
import unicodedata
import re
from typing import List, Tuple, Optional
from sqlalchemy.engine import Engine
from sqlalchemy import text
from rapidfuzz import process, fuzz


# -------------------------------------------
# Normalización y tokenización
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
    s = re.sub(r"[^a-z0-9\s]", " ", s)          # quitar signos/puntuación
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

def _score(a_norm: str, b_norm: str, a_toks: List[str], b_toks: List[str]) -> float:
    # 70% SequenceMatcher + 30% Jaccard
    return 0.7 * _seq_ratio(a_norm, b_norm) + 0.3 * _jaccard(a_toks, b_toks)

# -------------------------------------------
# Candidatos por SQL (ILIKE)
# -------------------------------------------
def _fetch_sql_candidates(
    engine: Engine,
    table_name: str,
    tokens: List[str],
    max_tokens_sql: int = 6,
    max_sql_candidates: int = 300
) -> pl.DataFrame:
    """
    Trae candidatos desde Postgres usando ILIKE por tokens (OR).
    - Usa hasta 'max_tokens_sql' tokens relevantes (>=3 chars).
    - Si no hay tokens largos, trae LIMIT max_sql_candidates sin filtro.
    - Limita 'max_sql_candidates' filas para rendimiento.
    """
    toks = [t for t in tokens if len(t) >= 3]
    if not toks:
        # Si no hay tokens largos, traer algunos candidatos sin filtro para RapidFuzz
        sql = f"""
            SELECT descripcion, concepto_ey, concepto_ey_global
            FROM {table_name}
            LIMIT :lim
        """
        params = {"lim": max_sql_candidates}
    else:
        # Reducir a los primeros N tokens (puedes mejorar con heurísticas de frecuencia)
        toks = toks[:max_tokens_sql]

        # Construir WHERE ... OR ... con parámetros seguros
        where_parts = []
        params = {}
        for i, t in enumerate(toks):
            key = f"p{i}"
            where_parts.append(f"descripcion ILIKE :{key}")
            params[key] = f"%{t}%"

        sql = f"""
            SELECT descripcion, concepto_ey, concepto_ey_global
            FROM {table_name}
            WHERE {" OR ".join(where_parts)}
            LIMIT :lim
        """
        params["lim"] = max_sql_candidates

    with engine.connect() as conn:
        # Usar pandas para leer y convertir a Polars
        df_pandas = pd.read_sql(text(sql), conn, params=params)
        return pl.from_pandas(df_pandas)

# -------------------------------------------
# Clasificación de una fila individual
# -------------------------------------------
def _clasificar_fila(
    a_norm: str,
    a_toks: List[str],
    engine: Engine,
    table_name: str,
    umbral: float,
    max_tokens_sql: int,
    max_sql_candidates: int
) -> Tuple[str, str, float]:
    """
    Clasifica una única descripción ya normalizada y tokenizada
    y devuelve (clasificacion, clasificacion_global, score).
    """
    # Asegurarse de trabajar con valores Python simples
    if isinstance(a_norm, pl.Series):
        a_norm = a_norm.to_list()[0] if a_norm.height > 0 else ""
    if isinstance(a_toks, pl.Series):
        # puede ser lista interna
        toks_list = a_toks.to_list()
        a_toks = toks_list[0] if len(toks_list) > 0 else []

    # Si no hay texto útil, devolver sin clasificación
    if isinstance(a_norm, str):
        a_norm = a_norm.strip() if a_norm else ""
    if not a_norm or (isinstance(a_toks, (list, tuple)) and len(a_toks) == 0):
        return ("Sin clasificacion", "", 0.0)

    # 1) Traer candidatos por SQL (ILIKE OR ...)
    cand = _fetch_sql_candidates(
        engine=engine,
        table_name=table_name,
        tokens=a_toks,
        max_tokens_sql=max_tokens_sql,
        max_sql_candidates=max_sql_candidates
    )

    if cand.is_empty():
        return ("Sin clasificacion", "", 0.0)

    # 2) Preparar descripciones normalizadas de candidatos
    cand = cand.with_columns(
        pl.col("descripcion").map_elements(_normalize_text, return_dtype=pl.Utf8).alias("desc_norm")
    )

    # 3) Usar RapidFuzz para encontrar el mejor match
    conceptos = cand["desc_norm"].to_list()
    resultado = process.extractOne(a_norm, conceptos, scorer=fuzz.ratio, score_cutoff=umbral * 100)

    if resultado is not None:
        mejor_match, score, match_idx = resultado
        # Obtener la fila del índice encontrado
        row_data = cand[match_idx]
        clasificacion = row_data["concepto_ey"].item() if row_data["concepto_ey"].is_not_null().item() else "Sin clasificacion"
        clasificacion_global = row_data["concepto_ey_global"].item() if row_data["concepto_ey_global"].is_not_null().item() else ""
        return (clasificacion, clasificacion_global, round(score / 100, 4))
    else:
        return ("Sin clasificacion", "", 0.0)


# -------------------------------------------
# Propagación de clasificación por proveedor_cliente
# -------------------------------------------
def _propagar_clasificacion_por_proveedor(df: pl.DataFrame, verbose: bool = True) -> pl.DataFrame:
    """
    Propaga clasificaciones basándose en proveedor_cliente.
    
    Si una fila tiene clasificacion != "Sin clasificacion" y un proveedor_cliente,
    asigna esa clasificación a todas las filas con el mismo proveedor_cliente
    que aún tienen "Sin clasificacion".
    """
    if "proveedor_cliente" not in df.columns:
        if verbose:
            print("⚠️  No se encontró la columna 'proveedor_cliente', se omite la propagación.")
        return df
    
    res = df.clone()
    
    # Encontrar filas con clasificación válida y proveedor_cliente no nulo
    clasificadas = res.filter(
        (pl.col("clasificacion") != "Sin clasificacion") & 
        pl.col("proveedor_cliente").is_not_null() &
        (pl.col("proveedor_cliente") != "")
    )
    
    if clasificadas.is_empty():
        if verbose:
            print("ℹ️  No hay clasificaciones válidas con proveedor_cliente para propagar.")
        return res
    
    # Crear diccionario: proveedor_cliente -> clasificacion
    proveedor_clasificacion = clasificadas.group_by("proveedor_cliente").agg(
        pl.col("clasificacion").first().alias("clasificacion")
    ).to_pandas().set_index("proveedor_cliente")["clasificacion"].to_dict()
    
    if verbose:
        print(f"📋 Encontrados {len(proveedor_clasificacion)} proveedores/clientes con clasificación para propagar.")
    
    # Propagar clasificaciones
    propagados = 0
    for proveedor, clasificacion in proveedor_clasificacion.items():
        mask_propagar = (
            (pl.col("proveedor_cliente") == proveedor) &
            (pl.col("clasificacion") == "Sin clasificacion")
        )
        cantidad = res.filter(mask_propagar).height
        if cantidad > 0:
            res = res.with_columns(
                pl.when(mask_propagar)
                .then(pl.lit(clasificacion))
                .otherwise(pl.col("clasificacion"))
                .alias("clasificacion")
            )
            propagados += cantidad
    
    if verbose:
        print(f"   → Clasificaciones propagadas: {propagados} registros")
    
    return res

# -------------------------------------------
# Clasificación principal (con iteración)
# -------------------------------------------
def Clasificarbalance(
    df: pl.DataFrame,
    engine: Engine,
    table_name: str = "ejmclasificaciones",
    umbral: float = 0.72,
    umbral_minimo: float = 0.35,
    paso_umbral: float = 0.05,
    max_tokens_sql: int = 6,
    max_sql_candidates: int = 300,
    max_iteraciones: int = 20,
    verbose: bool = True
) -> pl.DataFrame:
    """
    Clasifica df usando catálogo en Postgres con iteración automática.
    
    El clasificador itera bajando el umbral progresivamente hasta que:
      - No queden registros "Sin clasificacion", o
      - Se alcance el umbral_minimo, o
      - Se alcance el máximo de iteraciones
    
    Parámetros:
      - df: DataFrame con columna 'descripcion' o 'concepto'
      - engine: Engine de SQLAlchemy conectado a Postgres
      - table_name: Tabla con catálogo (descripcion, concepto_ey, concepto_ey_global)
      - umbral: Umbral inicial de similitud (default 0.72)
      - umbral_minimo: Umbral mínimo permitido (default 0.35)
      - paso_umbral: Reducción del umbral por iteración (default 0.05)
      - max_tokens_sql: Máximo de tokens para búsqueda SQL
      - max_sql_candidates: Máximo de candidatos SQL
      - max_iteraciones: Límite de iteraciones (default 20)
      - verbose: Mostrar progreso (default True)
    
    Devuelve SIEMPRE un DataFrame (nunca None).
    """
    # Columna de texto a clasificar
    texto_col = "descripcion" if "descripcion" in df.columns else ("concepto" if "concepto" in df.columns else None)
    if texto_col is None:
        raise ValueError("No se encontró columna 'descripcion' ni 'concepto' en el DataFrame.")

    # Inicializar resultados
    res = df.clone()
    # Si ya existe clasificacion, mantenerla; si no, inicializar
    if "clasificacion" not in res.columns:
        res = res.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))
    else:
        # Asegurar que los valores nulos o vacíos sean "Sin clasificacion"
        res = res.with_columns(
            pl.when(pl.col("clasificacion").is_null() | (pl.col("clasificacion") == ""))
            .then(pl.lit("Sin clasificacion"))
            .otherwise(pl.col("clasificacion"))
            .alias("clasificacion")
        )
    
    if "clasificacion_global" not in res.columns:
        res = res.with_columns(pl.lit("").alias("clasificacion_global"))
    if "score_clasificador" not in res.columns:
        res = res.with_columns(pl.lit(0.0).alias("score_clasificador"))
    
    # Paso previo: Propagar clasificaciones por proveedor_cliente
    if verbose:
        print("\n🔄 Propagando clasificaciones por proveedor_cliente...")
    res = _propagar_clasificacion_por_proveedor(res, verbose=verbose)
    
    # ----------------------------------------------------
    # PRE-CÁLCULO: normalizar y tokenizar texto una sola vez
    # ----------------------------------------------------
    texto_norm_col = "__texto_norm"
    texto_tokens_col = "__texto_tokens"

    if verbose:
        print("🧮 Normalizando y tokenizando texto base para clasificación...")

    res = res.with_columns(
        pl.col(texto_col).cast(pl.Utf8).map_elements(_normalize_text, return_dtype=pl.Utf8).alias(texto_norm_col),
        pl.col(texto_col).cast(pl.Utf8).map_elements(_tokenize, return_dtype=pl.List(pl.Utf8)).alias(texto_tokens_col)
    )

    if verbose:
        longitudes = res[texto_tokens_col].map_elements(len, return_dtype=pl.Int32)
        print(
            f"   - Registros con tokens vacíos: {longitudes.filter(longitudes == 0).len()} "
            f"de {res.height}"
        )
        print(
            f"   - Tokens promedio por registro (solo >0): "
            f"{longitudes.filter(longitudes > 0).mean():.2f}"
        )

    umbral_actual = umbral
    iteracion = 0
    
    while umbral_actual >= umbral_minimo and iteracion < max_iteraciones:
        iteracion += 1
        
        # Encontrar índices sin clasificar
        sin_clasificar = res.filter(pl.col("clasificacion") == "Sin clasificacion")
        indices_pendientes = sin_clasificar.select(pl.arange(0, pl.len()).alias("idx")).to_series().to_list()
        
        if not indices_pendientes:
            if verbose:
                print(f"✅ Iteración {iteracion}: Todos los registros clasificados.")
            break
        
        if verbose:
            print(
                f"🔄 Iteración {iteracion}: Umbral={umbral_actual:.2f}, "
                f"Pendientes={len(indices_pendientes)}"
            )
        
        clasificados_esta_iteracion = 0
        procesados_esta_iteracion = 0

        total_iter = len(indices_pendientes)

        for idx_pos, idx in enumerate(indices_pendientes, start=1):
            a_norm = res[texto_norm_col][idx]
            a_toks = res[texto_tokens_col][idx]

            clas, clas_global, score = _clasificar_fila(
                a_norm=a_norm,
                a_toks=a_toks,
                engine=engine,
                table_name=table_name,
                umbral=umbral_actual,
                max_tokens_sql=max_tokens_sql,
                max_sql_candidates=max_sql_candidates
            )
            
            if clas != "Sin clasificacion":
                res = res.with_columns(
                    pl.when(pl.arange(0, pl.len()) == idx)
                    .then(pl.lit(clas))
                    .otherwise(pl.col("clasificacion"))
                    .alias("clasificacion"),
                    pl.when(pl.arange(0, pl.len()) == idx)
                    .then(pl.lit(clas_global))
                    .otherwise(pl.col("clasificacion_global"))
                    .alias("clasificacion_global"),
                    pl.when(pl.arange(0, pl.len()) == idx)
                    .then(pl.lit(score))
                    .otherwise(pl.col("score_clasificador"))
                    .alias("score_clasificador")
                )
                clasificados_esta_iteracion += 1

            procesados_esta_iteracion += 1

            # Log de progreso cada 500 registros de esta iteración
            if verbose and (idx_pos % 500 == 0 or idx_pos == total_iter):
                pendientes_post = res.filter(pl.col("clasificacion") == "Sin clasificacion").height
                print(
                    f"      · Progreso iter {iteracion}: "
                    f"{idx_pos}/{total_iter} procesados "
                    f"(clasificados nuevos: {clasificados_esta_iteracion}, "
                    f"pendientes actuales: {pendientes_post})"
                )
        
        if verbose:
            print(f"   → Clasificados en esta iteración: {clasificados_esta_iteracion}")
        
        # Reducir umbral para siguiente iteración
        umbral_actual -= paso_umbral
        umbral_actual = round(umbral_actual, 2)  # Evitar errores de punto flotante
    
    # Resumen final
    # Eliminar columnas internas auxiliares
    res = res.drop([texto_norm_col, texto_tokens_col])

    sin_clasificar_final = res.filter(pl.col("clasificacion") == "Sin clasificacion").height
    if verbose:
        print(f"\n📊 Clasificación completada:")
        print(f"   - Total registros: {res.height}")
        print(f"   - Clasificados: {res.height - sin_clasificar_final}")
        print(f"   - Sin clasificar: {sin_clasificar_final}")
        print(f"   - Umbral final usado: {umbral_actual + paso_umbral:.2f}")
    
    return res
