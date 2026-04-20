from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from collections import Counter, defaultdict
from typing import Dict, List

import polars as pl
import pandas as pd
from rapidfuzz import fuzz
from sqlalchemy import text
from sqlalchemy.engine import Engine

# =========================================================
# Normalización / tokenización
# =========================================================

_STOPWORDS_ES = {
    "de","la","el","y","en","por","para","con","a","del","los","las","un","una","al",
    "o","u","que","se","su","sus","otros","otra","otro","sobre","le","les",
    "es","son","fue","ser","esta","este","esto","está","más","menos","muy",
    "me","mi","mis","tu","tus",
    # ruido bancario / operacional
    "transferencia","traspaso","mov","operacion","operación",
    "pago","referencia","banco","cta","cuenta","corriente","ahorro",
    "sa","s","ca","c","cia","compania","compañia",
}

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def _normalize_text(s: str) -> str:
    s = _strip_accents(str(s).lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(s: str) -> List[str]:
    if not s:
        return []
    return [t for t in s.split() if t and t not in _STOPWORDS_ES and len(t) >= 2]

def _extract_numbers(s: str) -> set[str]:
    return set(re.findall(r"\d+", s or ""))

# =========================================================
# Scoring híbrido
# =========================================================

def _build_idf(tokens_lists: List[List[str]]) -> Dict[str, float]:
    df_counter = Counter()
    total_docs = max(len(tokens_lists), 1)

    for toks in tokens_lists:
        for t in set(toks):
            df_counter[t] += 1

    idf = {}
    for t, df in df_counter.items():
        idf[t] = math.log((1 + total_docs) / (1 + df)) + 1.0
    return idf

def _weighted_overlap(a_toks: List[str], b_toks: List[str], idf: Dict[str, float]) -> float:
    sa, sb = set(a_toks), set(b_toks)
    if not sa or not sb:
        return 0.0

    inter = sa & sb
    union = sa | sb

    num = sum(idf.get(t, 1.0) for t in inter)
    den = sum(idf.get(t, 1.0) for t in union)
    return (num / den) if den else 0.0

def _hybrid_score(a_norm: str, b_norm: str, a_toks: List[str], b_toks: List[str], idf: Dict[str, float]) -> float:
    if not a_norm or not b_norm:
        return 0.0

    if a_norm == b_norm:
        return 1.0

    ratio = fuzz.ratio(a_norm, b_norm) / 100.0
    token_sort = fuzz.token_sort_ratio(a_norm, b_norm) / 100.0
    token_set = fuzz.token_set_ratio(a_norm, b_norm) / 100.0
    partial = fuzz.partial_ratio(a_norm, b_norm) / 100.0
    overlap = _weighted_overlap(a_toks, b_toks, idf)

    nums_a = _extract_numbers(a_norm)
    nums_b = _extract_numbers(b_norm)

    num_adjust = 0.0
    if nums_a and nums_b:
        if nums_a == nums_b:
            num_adjust = 0.03
        else:
            num_adjust = -0.08

    score = (
        0.18 * ratio +
        0.22 * token_sort +
        0.32 * token_set +
        0.10 * partial +
        0.18 * overlap
    ) + num_adjust

    return max(0.0, min(1.0, score))


# =========================================================
# Cache + índice de catálogo
# =========================================================

@dataclass
class CatalogIndex:
    df: pl.DataFrame
    exact_map: Dict[str, int]
    token_index: Dict[str, List[int]]
    idf: Dict[str, float]

_CATALOG_CACHE: Dict[str, CatalogIndex] = {}

def _load_catalog_index(engine: Engine, table_name: str) -> CatalogIndex:
    cache_key = table_name
    if cache_key in _CATALOG_CACHE:
        return _CATALOG_CACHE[cache_key]

    sql = f"""
        SELECT descripcion, concepto_ey, concepto_ey_global
        FROM {table_name}
        WHERE descripcion IS NOT NULL
          AND concepto_ey IS NOT NULL
    """

    with engine.connect() as conn:
        pdf = pd.read_sql(text(sql), conn)

    cat = pl.from_pandas(pdf)

    if cat.is_empty():
        idx = CatalogIndex(df=cat, exact_map={}, token_index={}, idf={})
        _CATALOG_CACHE[cache_key] = idx
        return idx

    cat = (
        cat
        .with_columns(
            pl.col("descripcion").cast(pl.Utf8).map_elements(_normalize_text, return_dtype=pl.Utf8).alias("desc_norm"),
            pl.col("descripcion").cast(pl.Utf8).map_elements(
                lambda x: _tokenize(_normalize_text(x)),
                return_dtype=pl.List(pl.Utf8)
            ).alias("desc_tokens")
        )
        .filter(pl.col("desc_norm").is_not_null() & (pl.col("desc_norm") != ""))
        .unique(subset=["desc_norm", "concepto_ey", "concepto_ey_global"], keep="first")
        .with_row_index("cat_idx")
    )

    exact_map: Dict[str, int] = {}
    token_index: Dict[str, List[int]] = defaultdict(list)

    tokens_lists = cat["desc_tokens"].to_list()

    for row in cat.select(["cat_idx", "desc_norm", "desc_tokens"]).iter_rows(named=True):
        exact_map.setdefault(row["desc_norm"], row["cat_idx"])
        for t in set(row["desc_tokens"] or []):
            if len(t) >= 3:
                token_index[t].append(row["cat_idx"])

    idf = _build_idf(tokens_lists)

    idx = CatalogIndex(
        df=cat,
        exact_map=exact_map,
        token_index=dict(token_index),
        idf=idf,
    )
    _CATALOG_CACHE[cache_key] = idx
    return idx


# =========================================================
# Propagación por proveedor_cliente (vectorizada)
# =========================================================

def _propagar_clasificacion_por_proveedor(df: pl.DataFrame, verbose: bool = True) -> pl.DataFrame:
    if "proveedor_cliente" not in df.columns:
        if verbose:
            print("⚠️ No se encontró 'proveedor_cliente'; se omite propagación.")
        return df

    fuente = df.filter(
        pl.col("proveedor_cliente").is_not_null() &
        (pl.col("proveedor_cliente") != "") &
        (pl.col("clasificacion") != "Sin clasificacion")
    )

    if fuente.is_empty():
        if verbose:
            print("ℹ️ No hay clasificaciones válidas por proveedor_cliente para propagar.")
        return df

    prov_map = (
        fuente
        .group_by("proveedor_cliente")
        .agg(
            pl.col("clasificacion").mode().first().alias("__prov_clas"),
            pl.col("clasificacion_global").mode().first().alias("__prov_clas_global"),
        )
    )

    res = (
        df.join(prov_map, on="proveedor_cliente", how="left")
          .with_columns(
              pl.when(
                  (pl.col("clasificacion") == "Sin clasificacion") &
                  pl.col("__prov_clas").is_not_null()
              )
              .then(pl.col("__prov_clas"))
              .otherwise(pl.col("clasificacion"))
              .alias("clasificacion"),

              pl.when(
                  (pl.col("clasificacion_global") == "") &
                  pl.col("__prov_clas_global").is_not_null()
              )
              .then(pl.col("__prov_clas_global"))
              .otherwise(pl.col("clasificacion_global"))
              .alias("clasificacion_global"),
          )
          .drop(["__prov_clas", "__prov_clas_global"])
    )

    if verbose:
        antes = df.filter(pl.col("clasificacion") != "Sin clasificacion").height
        despues = res.filter(pl.col("clasificacion") != "Sin clasificacion").height
        print(f"🔄 Propagadas {max(despues - antes, 0)} clasificaciones por proveedor_cliente.")

    return res


# =========================================================
# Selección de candidatos
# =========================================================

def _candidate_indices(tokens: List[str], idx: CatalogIndex, max_candidates: int = 300, max_seed_tokens: int = 4) -> List[int]:
    toks = list({t for t in tokens if len(t) >= 3 and t in idx.token_index})
    if not toks:
        return []

    # tokens raros primero
    toks.sort(key=lambda t: (len(idx.token_index[t]), -idx.idf.get(t, 1.0)))
    seeds = toks[:max_seed_tokens]

    candidate_set = set(idx.token_index[seeds[0]])
    for t in seeds[1:]:
        inter = candidate_set & set(idx.token_index[t])
        if inter:
            candidate_set = inter

    if not candidate_set:
        for t in seeds:
            candidate_set.update(idx.token_index[t])

    if len(candidate_set) > max_candidates:
        candidate_set = set(list(candidate_set)[:max_candidates])

    return list(candidate_set)


# =========================================================
# Clasificación de pendientes únicos
# =========================================================

def _classify_unique_pending(
    unique_pending: pl.DataFrame,
    catalog_idx: CatalogIndex,
    threshold: float,
    verbose: bool = True
) -> pl.DataFrame:

    if unique_pending.is_empty():
        return pl.DataFrame({
            "__texto_norm": [],
            "__match_clas": [],
            "__match_global": [],
            "__match_score": [],
        })

    cat = catalog_idx.df
    matched_rows = []

    rows = unique_pending.select(["__texto_norm", "__texto_tokens"]).iter_rows(named=True)
    rows = list(rows)
    total = len(rows)

    for i, row in enumerate(rows, start=1):
        a_norm = row["__texto_norm"]
        a_toks = row["__texto_tokens"] or []

        if not a_norm:
            continue

        # 1) Coincidencia exacta
        exact_idx = catalog_idx.exact_map.get(a_norm)
        if exact_idx is not None:
            exact_hit = cat.filter(pl.col("cat_idx") == exact_idx).select(
                pl.lit(a_norm).alias("__texto_norm"),
                pl.col("concepto_ey").first().alias("__match_clas"),
                pl.col("concepto_ey_global").first().alias("__match_global"),
                pl.lit(1.0).alias("__match_score"),
            )
            matched_rows.append(exact_hit)
            continue

        # 2) Candidatos por índice invertido
        cand_ids = _candidate_indices(a_toks, catalog_idx, max_candidates=300, max_seed_tokens=4)
        if not cand_ids:
            continue

        cand = cat.filter(pl.col("cat_idx").is_in(cand_ids)).select(
            ["desc_norm", "desc_tokens", "concepto_ey", "concepto_ey_global"]
        )

        best_score = -1.0
        best_clas = None
        best_global = None

        for c in cand.iter_rows(named=True):
            score = _hybrid_score(
                a_norm=a_norm,
                b_norm=c["desc_norm"],
                a_toks=a_toks,
                b_toks=c["desc_tokens"] or [],
                idf=catalog_idx.idf,
            )
            if score > best_score:
                best_score = score
                best_clas = c["concepto_ey"]
                best_global = c["concepto_ey_global"]

        if best_score >= threshold and best_clas:
            matched_rows.append(
                pl.DataFrame({
                    "__texto_norm": [a_norm],
                    "__match_clas": [best_clas],
                    "__match_global": [best_global or ""],
                    "__match_score": [round(best_score, 4)],
                })
            )

        if verbose and (i % 1000 == 0 or i == total):
            print(f" · Matching catálogo: {i}/{total} descripciones únicas")

    if not matched_rows:
        return pl.DataFrame({
            "__texto_norm": [],
            "__match_clas": [],
            "__match_global": [],
            "__match_score": [],
        })

    return pl.concat(matched_rows, how="vertical_relaxed").unique(subset=["__texto_norm"], keep="first")


# =========================================================
# API principal
# =========================================================

def Clasificarbalance(
    df: pl.DataFrame,
    engine: Engine,
    table_name: str = "ejmclasificaciones",
    umbral: float = 0.80,
    umbral_minimo: float = 0.35,
    paso_umbral: float = 0.05,
    max_iteraciones: int = 20,
    verbose: bool = True
) -> pl.DataFrame:
    """
    Clasifica un DataFrame usando catálogo precargado en memoria,
    score híbrido con RapidFuzz y actualización vectorizada por join.
    """

    texto_col = "descripcion" if "descripcion" in df.columns else ("concepto" if "concepto" in df.columns else None)
    if texto_col is None:
        raise ValueError("No se encontró columna 'descripcion' ni 'concepto' en el DataFrame.")

    res = df.clone()

    if "clasificacion" not in res.columns:
        res = res.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))
    else:
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

    # Propagación previa
    if verbose:
        print("\n🔄 Propagando clasificaciones por proveedor_cliente...")
    res = _propagar_clasificacion_por_proveedor(res, verbose=verbose)

    # Normalización / tokenización una sola vez
    if verbose:
        print("🧮 Normalizando y tokenizando texto base...")
    res = res.with_columns(
        pl.col(texto_col).cast(pl.Utf8).map_elements(_normalize_text, return_dtype=pl.Utf8).alias("__texto_norm"),
        pl.col(texto_col).cast(pl.Utf8).map_elements(
            lambda x: _tokenize(_normalize_text(x)),
            return_dtype=pl.List(pl.Utf8)
        ).alias("__texto_tokens")
    )

    catalog_idx = _load_catalog_index(engine, table_name)

    umbral_actual = umbral
    iteracion = 0

    while umbral_actual >= umbral_minimo and iteracion < max_iteraciones:
        iteracion += 1

        pendientes = (
            res
            .filter(pl.col("clasificacion") == "Sin clasificacion")
            .select(["__texto_norm", "__texto_tokens"])
            .filter(pl.col("__texto_norm").is_not_null() & (pl.col("__texto_norm") != ""))
            .unique(subset=["__texto_norm"])
        )

        if pendientes.is_empty():
            if verbose:
                print(f"✅ Iteración {iteracion}: no quedan pendientes.")
            break

        if verbose:
            print(f"🔄 Iteración {iteracion}: umbral={umbral_actual:.2f}, pendientes únicos={pendientes.height}")

        matches = _classify_unique_pending(
            unique_pending=pendientes,
            catalog_idx=catalog_idx,
            threshold=umbral_actual,
            verbose=verbose
        )

        if matches.is_empty():
            if verbose:
                print("   · Sin coincidencias nuevas en esta iteración.")
            umbral_actual = round(umbral_actual - paso_umbral, 2)
            continue

        antes = res.filter(pl.col("clasificacion") != "Sin clasificacion").height

        res = (
            res.join(matches, on="__texto_norm", how="left")
               .with_columns(
                   pl.when(
                       (pl.col("clasificacion") == "Sin clasificacion") &
                       pl.col("__match_clas").is_not_null()
                   )
                   .then(pl.col("__match_clas"))
                   .otherwise(pl.col("clasificacion"))
                   .alias("clasificacion"),

                   pl.when(
                       (pl.col("clasificacion_global") == "") &
                       pl.col("__match_global").is_not_null()
                   )
                   .then(pl.col("__match_global"))
                   .otherwise(pl.col("clasificacion_global"))
                   .alias("clasificacion_global"),

                   pl.when(
                       (pl.col("score_clasificador") == 0.0) &
                       pl.col("__match_score").is_not_null()
                   )
                   .then(pl.col("__match_score"))
                   .otherwise(pl.col("score_clasificador"))
                   .alias("score_clasificador"),
               )
               .drop(["__match_clas", "__match_global", "__match_score"])
        )

        despues = res.filter(pl.col("clasificacion") != "Sin clasificacion").height
        nuevos = despues - antes

        if verbose:
            print(f"   · Nuevas clasificaciones en iteración {iteracion}: {nuevos}")

        umbral_actual = round(umbral_actual - paso_umbral, 2)

    sin_clas = res.filter(pl.col("clasificacion") == "Sin clasificacion").height

    if verbose:
        print("\n📊 Clasificación principal completada")
        print(f" - Total registros: {res.height}")
        print(f" - Clasificados: {res.height - sin_clas}")
        print(f" - Sin clasificar: {sin_clas}")

    return res.drop(["__texto_norm", "__texto_tokens"])
