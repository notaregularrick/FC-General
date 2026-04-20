# Clasificador.py
import pandas as pd
import numpy as np
import unicodedata
import re
from difflib import SequenceMatcher
from typing import List, Tuple, Optional
from sqlalchemy.engine import Engine
from sqlalchemy import text

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
) -> pd.DataFrame:
    """
    Trae candidatos desde Postgres usando ILIKE por tokens (OR).
    - Usa hasta 'max_tokens_sql' tokens relevantes (>=3 chars).
    - Limita 'max_sql_candidates' filas para rendimiento.
    """
    toks = [t for t in tokens if len(t) >= 3]
    if not toks:
        return pd.DataFrame(columns=["descripcion", "concepto_ey", "concepto_ey_global"])

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
        return pd.read_sql(text(sql), conn, params=params)

# -------------------------------------------
# Clasificación de una fila individual
# -------------------------------------------
def _clasificar_fila(
    raw: str,
    engine: Engine,
    table_name: str,
    umbral: float,
    max_tokens_sql: int,
    max_sql_candidates: int
) -> Tuple[str, str, float]:
    """
    Clasifica una única descripción y devuelve (clasificacion, clasificacion_global, score).
    """
    a_norm = _normalize_text(raw)
    a_toks = _tokenize(a_norm)

    # 1) Traer candidatos por SQL (ILIKE OR ...)
    cand = _fetch_sql_candidates(
        engine=engine,
        table_name=table_name,
        tokens=a_toks,
        max_tokens_sql=max_tokens_sql,
        max_sql_candidates=max_sql_candidates
    )

    if cand.empty:
        return ("Sin clasificacion", "", 0.0)

    # 2) Preparar normalización de candidatos
    cand["desc_norm"]   = cand["descripcion"].astype(str).map(_normalize_text)
    cand["desc_tokens"] = cand["desc_norm"].map(_tokenize)

    # 3) Buscar mejor score
    best_s = -1.0
    best_idx = -1
    for i, row in cand.iterrows():
        s = _score(a_norm, row["desc_norm"], a_toks, row["desc_tokens"])
        if s > best_s:
            best_s, best_idx = s, i

    if best_s >= umbral and best_idx != -1:
        row = cand.loc[best_idx]
        return (row["concepto_ey"], row.get("concepto_ey_global", ""), round(best_s, 4))
    else:
        return ("Sin clasificacion", "", round(best_s if best_s > 0 else 0.0, 4))


# -------------------------------------------
# Propagación de clasificación por proveedor_cliente
# -------------------------------------------
def _propagar_clasificacion_por_proveedor(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
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
    
    res = df.copy()
    
    # Encontrar filas con clasificación válida y proveedor_cliente no nulo
    mask_clasificadas = (
        (res["clasificacion"] != "Sin clasificacion") & 
        (res["proveedor_cliente"].notna()) &
        (res["proveedor_cliente"] != "")
    )
    
    if not mask_clasificadas.any():
        if verbose:
            print("ℹ️  No hay clasificaciones válidas con proveedor_cliente para propagar.")
        return res
    
    # Crear diccionario: proveedor_cliente -> clasificacion
    # Si hay múltiples clasificaciones para el mismo proveedor, tomamos la primera encontrada
    proveedor_clasificacion = {}
    for idx in res[mask_clasificadas].index:
        proveedor = res.at[idx, "proveedor_cliente"]
        clasificacion = res.at[idx, "clasificacion"]
        if proveedor not in proveedor_clasificacion:
            proveedor_clasificacion[proveedor] = clasificacion
    
    if verbose:
        print(f"📋 Encontrados {len(proveedor_clasificacion)} proveedores/clientes con clasificación para propagar.")
    
    # Propagar clasificaciones
    propagados = 0
    for proveedor, clasificacion in proveedor_clasificacion.items():
        # Filas con el mismo proveedor_cliente que aún tienen "Sin clasificacion"
        mask_propagar = (
            (res["proveedor_cliente"] == proveedor) &
            (res["clasificacion"] == "Sin clasificacion")
        )
        
        if mask_propagar.any():
            cantidad = mask_propagar.sum()
            res.loc[mask_propagar, "clasificacion"] = clasificacion
            propagados += cantidad
    
    if verbose:
        print(f"   → Clasificaciones propagadas: {propagados} registros")
    
    return res

# -------------------------------------------
# Clasificación principal (con iteración)
# -------------------------------------------
def Clasificarbalance(
    df: pd.DataFrame,
    engine: Engine,
    table_name: str = "ejmclasificaciones",
    umbral: float = 0.72,
    umbral_minimo: float = 0.35,
    paso_umbral: float = 0.05,
    max_tokens_sql: int = 6,
    max_sql_candidates: int = 300,
    max_iteraciones: int = 20,
    verbose: bool = True
) -> pd.DataFrame:
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
    res = df.copy()
    # Si ya existe clasificacion, mantenerla; si no, inicializar
    if "clasificacion" not in res.columns:
        res["clasificacion"] = "Sin clasificacion"
    else:
        # Asegurar que los valores nulos o vacíos sean "Sin clasificacion"
        res["clasificacion"] = res["clasificacion"].fillna("Sin clasificacion")
        res.loc[res["clasificacion"] == "", "clasificacion"] = "Sin clasificacion"
    
    if "clasificacion_global" not in res.columns:
        res["clasificacion_global"] = ""
    if "score_clasificador" not in res.columns:
        res["score_clasificador"] = 0.0
    
    # Paso previo: Propagar clasificaciones por proveedor_cliente
    if verbose:
        print("\n🔄 Propagando clasificaciones por proveedor_cliente...")
    res = _propagar_clasificacion_por_proveedor(res, verbose=verbose)
    
    umbral_actual = umbral
    iteracion = 0
    
    while umbral_actual >= umbral_minimo and iteracion < max_iteraciones:
        iteracion += 1
        
        # Encontrar índices sin clasificar
        mask_sin_clasificar = res["clasificacion"] == "Sin clasificacion"
        indices_pendientes = res[mask_sin_clasificar].index.tolist()
        
        if not indices_pendientes:
            if verbose:
                print(f"✅ Iteración {iteracion}: Todos los registros clasificados.")
            break
        
        if verbose:
            print(f"🔄 Iteración {iteracion}: Umbral={umbral_actual:.2f}, Pendientes={len(indices_pendientes)}")
        
        clasificados_esta_iteracion = 0
        
        for idx in indices_pendientes:
            raw = str(res.at[idx, texto_col]) if pd.notna(res.at[idx, texto_col]) else ""
            
            clas, clas_global, score = _clasificar_fila(
                raw=raw,
                engine=engine,
                table_name=table_name,
                umbral=umbral_actual,
                max_tokens_sql=max_tokens_sql,
                max_sql_candidates=max_sql_candidates
            )
            
            if clas != "Sin clasificacion":
                res.at[idx, "clasificacion"] = clas
                res.at[idx, "clasificacion_global"] = clas_global
                res.at[idx, "score_clasificador"] = score
                clasificados_esta_iteracion += 1
        
        if verbose:
            print(f"   → Clasificados en esta iteración: {clasificados_esta_iteracion}")
        
        # Reducir umbral para siguiente iteración
        umbral_actual -= paso_umbral
        umbral_actual = round(umbral_actual, 2)  # Evitar errores de punto flotante
    
    # Resumen final
    sin_clasificar_final = (res["clasificacion"] == "Sin clasificacion").sum()
    if verbose:
        print(f"\n📊 Clasificación completada:")
        print(f"   - Total registros: {len(res)}")
        print(f"   - Clasificados: {len(res) - sin_clasificar_final}")
        print(f"   - Sin clasificar: {sin_clasificar_final}")
        print(f"   - Umbral final usado: {umbral_actual + paso_umbral:.2f}")
    
    return res
