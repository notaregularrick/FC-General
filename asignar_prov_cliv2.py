from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from collections import Counter, defaultdict
from typing import Dict, List, Set, Optional

import pandas as pd
import polars as pl
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
    # ruido de razón social / dominio
    "sa","s","ca","c","cia","compania","compañia","group","holding","holdings",
    "servicios","inversiones","inversion","comercial","comerciales","industrial",
    "industriales","distribuidora","distribuciones","corporacion","corporación",
}

def _normalizar(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(s: str) -> List[str]:
    if not s:
        return []
    return [t for t in s.split() if t and t not in _STOPWORDS_ES and len(t) >= 2]

def _extract_numbers(s: str) -> Set[str]:
    return set(re.findall(r"\d+", s or ""))


# =========================================================
# Score para elegir mejor proveedor entre candidatos
# =========================================================

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

def _score_proveedor(desc_norm: str, nombre_norm: str, desc_toks: List[str], nombre_toks: List[str], idf: Dict[str, float]) -> float:
    """
    Score para escoger el mejor proveedor cuando ya redujimos candidatos
    con índice invertido.
    """

    if not desc_norm or not nombre_norm:
        return 0.0

    if nombre_norm in desc_norm:
        # nombre completo contenido = señal muy fuerte
        base = 0.92
    else:
        base = 0.0

    ratio = fuzz.ratio(desc_norm, nombre_norm) / 100.0
    partial = fuzz.partial_ratio(desc_norm, nombre_norm) / 100.0
    token_set = fuzz.token_set_ratio(desc_norm, nombre_norm) / 100.0
    overlap = _weighted_overlap(desc_toks, nombre_toks, idf)

    nums_desc = _extract_numbers(desc_norm)
    nums_nombre = _extract_numbers(nombre_norm)

    num_adjust = 0.0
    if nums_nombre:
        if nums_nombre.issubset(nums_desc):
            num_adjust = 0.03
        else:
            num_adjust = -0.05

    score = max(
        base,
        (0.15 * ratio + 0.30 * partial + 0.35 * token_set + 0.20 * overlap) + num_adjust
    )

    return max(0.0, min(1.0, score))


# =========================================================
# Índice invertido de proveedores
# =========================================================

@dataclass
class ProviderIndex:
    proveedores_df: pl.DataFrame
    token_index: Dict[str, List[int]]
    exact_name_map: Dict[str, int]
    rif_map: Dict[str, int]
    idf: Dict[str, float]

_PROVIDER_CACHE: Dict[str, ProviderIndex] = {}

def _build_provider_index(engine: Engine, cache_key: str = "cliente_proveedores") -> ProviderIndex:
    if cache_key in _PROVIDER_CACHE:
        return _PROVIDER_CACHE[cache_key]

    query = """
        SELECT nombre, rif
        FROM cliente_proveedores
        WHERE nombre IS NOT NULL
    """

    with engine.connect() as conn:
        pdf = pd.read_sql(text(query), conn)

    proveedores = pl.from_pandas(pdf)

    if proveedores.is_empty():
        idx = ProviderIndex(
            proveedores_df=proveedores,
            token_index={},
            exact_name_map={},
            rif_map={},
            idf={}
        )
        _PROVIDER_CACHE[cache_key] = idx
        return idx

    proveedores = (
        proveedores
        .drop_nulls("nombre")
        .with_columns(
            pl.col("nombre").cast(pl.Utf8).alias("nombre"),
            pl.col("nombre").cast(pl.Utf8).map_elements(_normalizar, return_dtype=pl.Utf8).alias("nombre_norm"),
            pl.col("rif").fill_null("").cast(pl.Utf8).map_elements(_normalizar, return_dtype=pl.Utf8).alias("rif_norm"),
        )
        .filter(pl.col("nombre_norm").str.len_chars() >= 3)
        .unique(subset=["nombre_norm", "rif_norm"], keep="first")
        .with_row_index("prov_idx")
    )

    token_index: Dict[str, List[int]] = defaultdict(list)
    exact_name_map: Dict[str, int] = {}
    rif_map: Dict[str, int] = {}

    tokens_lists = []

    for row in proveedores.select(["prov_idx", "nombre", "nombre_norm", "rif_norm"]).iter_rows(named=True):
        prov_idx = row["prov_idx"]
        nombre_norm = row["nombre_norm"]
        rif_norm = row["rif_norm"]

        toks = _tokenize(nombre_norm)
        tokens_lists.append(toks)

        exact_name_map.setdefault(nombre_norm, prov_idx)

        if rif_norm and len(rif_norm) >= 5:
            rif_map.setdefault(rif_norm, prov_idx)

        for t in set(toks):
            if len(t) >= 3:
                token_index[t].append(prov_idx)

    idf = _build_idf(tokens_lists)

    # agregamos tokens para evitar recalcularlos luego
    proveedores = proveedores.with_columns(
        pl.col("nombre_norm").map_elements(_tokenize, return_dtype=pl.List(pl.Utf8)).alias("nombre_tokens")
    )

    idx = ProviderIndex(
        proveedores_df=proveedores,
        token_index=dict(token_index),
        exact_name_map=exact_name_map,
        rif_map=rif_map,
        idf=idf
    )
    _PROVIDER_CACHE[cache_key] = idx
    return idx


# =========================================================
# Selección de candidatos usando índice invertido
# =========================================================

def _candidate_provider_indices(desc_toks: List[str], idx: ProviderIndex, max_candidates: int = 150, max_seed_tokens: int = 4) -> List[int]:
    """
    Usa tokens raros primero para acotar candidatos.
    """

    toks = list({t for t in desc_toks if len(t) >= 3 and t in idx.token_index})
    if not toks:
        return []

    # raros primero = listas más pequeñas + más discriminantes
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
# Resolver mejor proveedor para una descripción única
# =========================================================

def _match_provider_for_description(desc_norm: str, desc_toks: List[str], pidx: ProviderIndex, score_threshold: float = 0.78):
    """
    Devuelve el nombre del mejor proveedor para una descripción normalizada,
    o None si no encuentra uno suficientemente confiable.
    """

    if not desc_norm:
        return None

    prov_df = pidx.proveedores_df

    # -----------------------------------------------------
    # 1) Match por RIF (máxima prioridad)
    # -----------------------------------------------------
    for rif_norm, prov_idx in pidx.rif_map.items():
        if rif_norm in desc_norm:
            row = prov_df.filter(pl.col("prov_idx") == prov_idx).select("nombre").to_series()
            return row[0] if len(row) > 0 else None

    # -----------------------------------------------------
    # 2) Match exacto por nombre completo contenido
    #    (priorizando nombres más largos)
    # -----------------------------------------------------
    # Acotar primero por tokens usando índice invertido
    cand_ids = _candidate_provider_indices(desc_toks, pidx, max_candidates=150, max_seed_tokens=4)

    if cand_ids:
        cand = (
            prov_df
            .filter(pl.col("prov_idx").is_in(cand_ids))
            .with_columns(pl.col("nombre_norm").str.len_chars().alias("__len_nombre"))
            .sort("__len_nombre", descending=True)
        )

        # nombre completo contenido
        for row in cand.select(["nombre", "nombre_norm"]).iter_rows(named=True):
            if row["nombre_norm"] and row["nombre_norm"] in desc_norm:
                return row["nombre"]

        # -------------------------------------------------
        # 3) Elegir mejor candidato por score
        # -------------------------------------------------
        best_score = -1.0
        best_name = None

        for row in cand.select(["nombre", "nombre_norm", "nombre_tokens"]).iter_rows(named=True):
            score = _score_proveedor(
                desc_norm=desc_norm,
                nombre_norm=row["nombre_norm"],
                desc_toks=desc_toks,
                nombre_toks=row["nombre_tokens"] or [],
                idf=pidx.idf
            )
            if score > best_score:
                best_score = score
                best_name = row["nombre"]

        if best_score >= score_threshold:
            return best_name

    return None


# =========================================================
# API principal
# =========================================================

def asignar_prov_cli(df: pl.DataFrame, engine: Engine, verbose: bool = True, score_threshold: float = 0.78) -> pl.DataFrame:
    """
    Asigna proveedor_cliente usando:
    1) RIF contenido en la descripción
    2) nombre completo contenido
    3) índice invertido por tokens + score entre candidatos

    Optimizaciones:
    - cache de cliente_proveedores
    - normalización una sola vez
    - matching por descripciones únicas
    - join vectorizado final
    """

    res = df.clone()

    if "descripcion" not in res.columns:
        if verbose:
            print("⚠️ No existe la columna 'descripcion'.")
        return res

    if "proveedor_cliente" not in res.columns:
        res = res.with_columns(pl.lit("").alias("proveedor_cliente"))
    else:
        res = res.with_columns(
            pl.col("proveedor_cliente").cast(pl.Utf8).fill_null("").alias("proveedor_cliente")
        )

    pidx = _build_provider_index(engine)

    if pidx.proveedores_df.is_empty():
        if verbose:
            print("⚠️ No hay proveedores válidos en cliente_proveedores.")
        return res

    # Normalizar descripción una sola vez
    res = res.with_columns(
        pl.col("descripcion").cast(pl.Utf8).fill_null("").map_elements(_normalizar, return_dtype=pl.Utf8).alias("__desc_norm"),
        pl.col("descripcion").cast(pl.Utf8).fill_null("").map_elements(
            lambda x: _tokenize(_normalizar(x)),
            return_dtype=pl.List(pl.Utf8)
        ).alias("__desc_tokens")
    )

    # Solo descripciones únicas pendientes
    pendientes = (
        res
        .filter(pl.col("proveedor_cliente") == "")
        .select(["__desc_norm", "__desc_tokens"])
        .filter(pl.col("__desc_norm") != "")
        .unique(subset=["__desc_norm"])
    )

    if pendientes.is_empty():
        if verbose:
            print("ℹ️ No hay filas pendientes para asignar proveedor_cliente.")
        return res.drop(["__desc_norm", "__desc_tokens"])

    rows = list(pendientes.iter_rows(named=True))
    total = len(rows)

    matches = []
    rif_hits = 0
    exact_name_hits = 0
    fuzzy_index_hits = 0

    for i, row in enumerate(rows, start=1):
        desc_norm = row["__desc_norm"]
        desc_toks = row["__desc_tokens"] or []

        proveedor_match = None

        # Hacemos el match paso a paso para poder llevar métricas
        # 1) rif
        for rif_norm, prov_idx in pidx.rif_map.items():
            if rif_norm in desc_norm:
                serie = (
                    pidx.proveedores_df
                    .filter(pl.col("prov_idx") == prov_idx)
                    .select("nombre")
                    .to_series()
                )
                proveedor_match = serie[0] if len(serie) > 0 else None
                if proveedor_match:
                    rif_hits += 1
                break

        # 2) exact name / 3) index + score
        if proveedor_match is None:
            cand_ids = _candidate_provider_indices(desc_toks, pidx, max_candidates=150, max_seed_tokens=4)

            if cand_ids:
                cand = (
                    pidx.proveedores_df
                    .filter(pl.col("prov_idx").is_in(cand_ids))
                    .with_columns(pl.col("nombre_norm").str.len_chars().alias("__len_nombre"))
                    .sort("__len_nombre", descending=True)
                )

                # 2) nombre completo contenido
                for c in cand.select(["nombre", "nombre_norm"]).iter_rows(named=True):
                    if c["nombre_norm"] and c["nombre_norm"] in desc_norm:
                        proveedor_match = c["nombre"]
                        exact_name_hits += 1
                        break

                # 3) mejor score sobre candidatos reducidos
                if proveedor_match is None:
                    best_score = -1.0
                    best_name = None

                    for c in cand.select(["nombre", "nombre_norm", "nombre_tokens"]).iter_rows(named=True):
                        score = _score_proveedor(
                            desc_norm=desc_norm,
                            nombre_norm=c["nombre_norm"],
                            desc_toks=desc_toks,
                            nombre_toks=c["nombre_tokens"] or [],
                            idf=pidx.idf
                        )
                        if score > best_score:
                            best_score = score
                            best_name = c["nombre"]

                    if best_score >= score_threshold:
                        proveedor_match = best_name
                        fuzzy_index_hits += 1

        if proveedor_match is not None:
            matches.append({
                "__desc_norm": desc_norm,
                "__prov_match": proveedor_match
            })

        if verbose and (i % 1000 == 0 or i == total):
            print(
                f" · Progreso descripciones únicas: {i}/{total} "
                f"(matches={len(matches)}, rif={rif_hits}, exact_name={exact_name_hits}, index_score={fuzzy_index_hits})"
            )

    if not matches:
        if verbose:
            print("✅ Asignación completada: no hubo coincidencias nuevas.")
        return res.drop(["__desc_norm", "__desc_tokens"])

    match_df = pl.DataFrame(matches).unique(subset=["__desc_norm"], keep="first")

    antes = res.filter(pl.col("proveedor_cliente") != "").height

    res = (
        res.join(match_df, on="__desc_norm", how="left")
           .with_columns(
               pl.when(
                   (pl.col("proveedor_cliente") == "") &
                   pl.col("__prov_match").is_not_null()
               )
               .then(pl.col("__prov_match"))
               .otherwise(pl.col("proveedor_cliente"))
               .alias("proveedor_cliente")
           )
           .drop(["__prov_match", "__desc_norm", "__desc_tokens"])
    )

    despues = res.filter(pl.col("proveedor_cliente") != "").height
    nuevos = max(despues - antes, 0)

    if verbose:
        print("📊 Resultado asignar_prov_cli con índice invertido:")
        print(f" - Descripciones únicas evaluadas: {total}")
        print(f" - Matches por RIF: {rif_hits}")
        print(f" - Matches por nombre exacto: {exact_name_hits}")
        print(f" - Matches por índice+score: {fuzzy_index_hits}")
        print(f" - Filas nuevas con proveedor_cliente asignado: {nuevos}")

    return res