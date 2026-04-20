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

_STOPWORDS_ES = {
    "de","la","el","y","en","por","para","con","a","del","los","las","un","una","al",
    "o","u","que","se","su","sus","otros","otra","otro","sobre","le","les",
    "es","son","fue","ser","esta","este","esto","está","más","menos","muy",
    "me","mi","mis","tu","tus",
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

def _build_idf(tokens_lists: List[List[str]]) -> Dict[str, float]:
    df_counter = Counter()
    total_docs = max(len(tokens_lists), 1)

    for toks in tokens_lists:
        for t in set(toks):
            df_counter[t] += 1

    return {
        t: math.log((1 + total_docs) / (1 + df)) + 1.0
        for t, df in df_counter.items()
    }

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
        num_adjust = 0.03 if nums_a == nums_b else -0.08

    score = (
        0.18 * ratio +
        0.22 * token_sort +
        0.32 * token_set +
        0.10 * partial +
        0.18 * overlap
    ) + num_adjust

    return max(0.0, min(1.0, score))


@dataclass
class HistoricIndex:
    df: pl.DataFrame
    exact_map: Dict[str, int]
    token_index: Dict[str, List[int]]
    idf: Dict[str, float]

_HIST_CACHE: Dict[str, HistoricIndex] = {}


def _load_historic_index(engine: Engine, cache_key: str = "balance_general") -> HistoricIndex:
    if cache_key in _HIST_CACHE:
        return _HIST_CACHE[cache_key]

    query = """
        SELECT descripcion, clasificacion, proveedor_cliente
        FROM balance_general
        WHERE descripcion IS NOT NULL
          AND clasificacion IS NOT NULL
          AND clasificacion <> 'Sin clasificacion'
    """

    with engine.connect() as conn:
        pdf = pd.read_sql(text(query), conn)

    hist = pl.from_pandas(pdf)

    if hist.is_empty():
        idx = HistoricIndex(df=hist, exact_map={}, token_index={}, idf={})
        _HIST_CACHE[cache_key] = idx
        return idx

    hist = (
        hist
        .with_columns(
            pl.col("descripcion").cast(pl.Utf8).map_elements(_normalize_text, return_dtype=pl.Utf8).alias("desc_norm"),
            pl.col("descripcion").cast(pl.Utf8).map_elements(
                lambda x: _tokenize(_normalize_text(x)),
                return_dtype=pl.List(pl.Utf8)
            ).alias("desc_tokens"),
        )
        .filter(pl.col("desc_norm").is_not_null() & (pl.col("desc_norm") != ""))
        .group_by("desc_norm")
        .agg(
            pl.col("desc_tokens").first().alias("desc_tokens"),
            pl.col("clasificacion").mode().first().alias("clasificacion"),
            pl.col("proveedor_cliente").drop_nulls().first().alias("proveedor_cliente"),
        )
        .with_row_index("hist_idx")
    )

    exact_map: Dict[str, int] = {}
    token_index: Dict[str, List[int]] = defaultdict(list)

    tokens_lists = hist["desc_tokens"].to_list()

    for row in hist.select(["hist_idx", "desc_norm", "desc_tokens"]).iter_rows(named=True):
        exact_map.setdefault(row["desc_norm"], row["hist_idx"])
        for t in set(row["desc_tokens"] or []):
            if len(t) >= 3:
                token_index[t].append(row["hist_idx"])

    idf = _build_idf(tokens_lists)

    idx = HistoricIndex(
        df=hist,
        exact_map=exact_map,
        token_index=dict(token_index),
        idf=idf
    )
    _HIST_CACHE[cache_key] = idx
    return idx


def _candidate_indices(tokens: List[str], idx: HistoricIndex, max_candidates: int = 300, max_seed_tokens: int = 4) -> List[int]:
    toks = list({t for t in tokens if len(t) >= 3 and t in idx.token_index})
    if not toks:
        return []

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


def clasificar_con_balance_historico(
    df: pl.DataFrame,
    engine: Engine,
    umbral_similitud: float = 0.85,
    verbose: bool = True
) -> pl.DataFrame:
    """
    Clasifica usando balance_general histórico precargado/caché,
    score híbrido y actualización vectorizada por join.
    """

    if "descripcion" not in df.columns:
        if verbose:
            print("⚠️ No se encontró la columna 'descripcion'.")
        return df

    res = df.clone()

    if "clasificacion" not in res.columns:
        res = res.with_columns(pl.lit("Sin clasificacion").alias("clasificacion"))

    if "proveedor_cliente" not in res.columns:
        res = res.with_columns(pl.lit(None).alias("proveedor_cliente"))

    hist_idx = _load_historic_index(engine)
    hist = hist_idx.df

    if hist.is_empty():
        if verbose:
            print("ℹ️ No hay histórico con clasificaciones válidas.")
        return res

    if verbose:
        print("🧮 Normalizando y tokenizando descripciones actuales...")

    res = res.with_columns(
        pl.col("descripcion").cast(pl.Utf8).map_elements(_normalize_text, return_dtype=pl.Utf8).alias("__desc_norm"),
        pl.col("descripcion").cast(pl.Utf8).map_elements(
            lambda x: _tokenize(_normalize_text(x)),
            return_dtype=pl.List(pl.Utf8)
        ).alias("__desc_tokens"),
    )

    pendientes = (
        res
        .filter(pl.col("clasificacion") == "Sin clasificacion")
        .select(["__desc_norm", "__desc_tokens"])
        .filter(pl.col("__desc_norm").is_not_null() & (pl.col("__desc_norm") != ""))
        .unique(subset=["__desc_norm"])
    )

    if pendientes.is_empty():
        return res.drop(["__desc_norm", "__desc_tokens"])

    matched = []
    rows = list(pendientes.iter_rows(named=True))
    total = len(rows)

    if verbose:
        print(f"🔎 Histórico: pendientes únicos = {total}")

    for i, row in enumerate(rows, start=1):
        a_norm = row["__desc_norm"]
        a_toks = row["__desc_tokens"] or []

        if not a_norm:
            continue

        # exacto
        exact_idx = hist_idx.exact_map.get(a_norm)
        if exact_idx is not None:
            hit = hist.filter(pl.col("hist_idx") == exact_idx).select(
                pl.lit(a_norm).alias("__desc_norm"),
                pl.col("clasificacion").first().alias("__hist_clas"),
                pl.col("proveedor_cliente").first().alias("__hist_prov"),
                pl.lit(1.0).alias("__hist_score"),
            )
            matched.append(hit)
            continue

        cand_ids = _candidate_indices(a_toks, hist_idx, max_candidates=300, max_seed_tokens=4)
        if not cand_ids:
            continue

        cand = hist.filter(pl.col("hist_idx").is_in(cand_ids)).select(
            ["desc_norm", "desc_tokens", "clasificacion", "proveedor_cliente"]
        )

        best_score = -1.0
        best_clas = None
        best_prov = None

        for c in cand.iter_rows(named=True):
            score = _hybrid_score(
                a_norm=a_norm,
                b_norm=c["desc_norm"],
                a_toks=a_toks,
                b_toks=c["desc_tokens"] or [],
                idf=hist_idx.idf
            )
            if score > best_score:
                best_score = score
                best_clas = c["clasificacion"]
                best_prov = c["proveedor_cliente"]

        if best_score >= umbral_similitud and best_clas:
            matched.append(
                pl.DataFrame({
                    "__desc_norm": [a_norm],
                    "__hist_clas": [best_clas],
                    "__hist_prov": [best_prov],
                    "__hist_score": [round(best_score, 4)],
                })
            )

        if verbose and (i % 1000 == 0 or i == total):
            print(f" · Matching histórico: {i}/{total} descripciones únicas")

    if matched:
        mdf = pl.concat(matched, how="vertical_relaxed").unique(subset=["__desc_norm"], keep="first")

        antes = res.filter(pl.col("clasificacion") != "Sin clasificacion").height

        res = (
            res.join(mdf, on="__desc_norm", how="left")
               .with_columns(
                   pl.when(
                       (pl.col("clasificacion") == "Sin clasificacion") &
                       pl.col("__hist_clas").is_not_null()
                   )
                   .then(pl.col("__hist_clas"))
                   .otherwise(pl.col("clasificacion"))
                   .alias("clasificacion"),

                   pl.when(
                       pl.col("proveedor_cliente").is_null() &
                       pl.col("__hist_prov").is_not_null()
                   )
                   .then(pl.col("__hist_prov"))
                   .otherwise(pl.col("proveedor_cliente"))
                   .alias("proveedor_cliente"),
               )
               .drop(["__hist_clas", "__hist_prov", "__hist_score"])
        )

        despues = res.filter(pl.col("clasificacion") != "Sin clasificacion").height
        if verbose:
            print(f"📚 Clasificaciones históricas nuevas: {max(despues - antes, 0)}")
    else:
        if verbose:
            print("📚 No hubo coincidencias históricas nuevas.")

    return res.drop(["__desc_norm", "__desc_tokens"])