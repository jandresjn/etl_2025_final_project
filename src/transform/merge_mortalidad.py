# Uso: python -m src.transform.merge_mortalidad
# Une mortalidad (OSB) con agregados 2018 por localidad, tolerante a LOCALIDAD_{left,right,JOIN}.

import os
import pandas as pd

RAW_MORT = "data/raw/mortalidad/mortalidad_raw.parquet"
CLEAN = "data/clean"
ANAL = "data/analytics"
os.makedirs(ANAL, exist_ok=True)

OUT_PANEL_2018 = f"{ANAL}/panel_localidad_2018.parquet"


def _norm_localidad(s):
    return str(s).strip().upper()


def _pick_loc_col(cols):
    up = [c for c in cols if "LOCALIDAD" in c.upper()]
    if not up:
        return None
    # orden de preferencia: *_JOIN > *_right > *_left > LOCALIDAD exacto
    pref = sorted(
        up,
        key=lambda c: (
            0
            if c.lower().endswith("_join")
            else (
                1
                if c.lower().endswith("_right")
                else 2 if c.lower().endswith("_left") else 3
            )
        ),
    )
    return pref[0]


def _ensure_loc_join(df, tag):
    col = _pick_loc_col(df.columns)
    if not col:
        raise KeyError(
            f"No encuentro columna de localidad en {tag}. Columnas: {list(df.columns)[:20]}"
        )
    if col != "LOCALIDAD_JOIN":
        df = df.rename(columns={col: "LOCALIDAD_JOIN"})
    df["LOCALIDAD_JOIN"] = df["LOCALIDAD_JOIN"].map(_norm_localidad)
    return df


def _group_by_localidad(df, tag):
    df = _ensure_loc_join(df, tag)
    g = df.groupby("LOCALIDAD_JOIN").size().rename(f"{tag}_2018")
    return g


def main():
    # --- Mortalidad ---
    mort = pd.read_parquet(RAW_MORT).copy()
    mort.columns = [
        c.strip().lower().replace(" ", "_").replace(".", "_") for c in mort.columns
    ]

    # normaliza campos esperados
    for c in list(mort.columns):
        if c.startswith("tasa_x_100") and "habitantes" in c:
            mort = mort.rename(columns={c: "tasa_x_100k"})
    if "ano" not in mort.columns and "año" in mort.columns:
        mort = mort.rename(columns={"año": "ano"})

    mort["localidad"] = mort.get("localidad", "").map(_norm_localidad)
    mort["ano"] = pd.to_numeric(mort.get("ano"), errors="coerce").astype("Int64")
    mort["casos"] = pd.to_numeric(mort.get("casos"), errors="coerce")
    mort["poblacion"] = pd.to_numeric(mort.get("poblacion"), errors="coerce")

    mort18 = mort[mort["ano"] == 2018].copy()

    # --- Agregados por localidad (comparendos/siniestros) ---
    comp = pd.read_parquet(f"{CLEAN}/comparendos_2018_loc.parquet")
    sin = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_loc.parquet")

    comp_g = _group_by_localidad(comp, "comparendos")
    sin_g = _group_by_localidad(sin, "siniestros")

    base = pd.concat(
        [comp_g, sin_g], axis=1
    ).reset_index()  # LOCALIDAD_JOIN, comparendos_2018, siniestros_2018

    # --- Merge con mortalidad 2018 ---
    panel = base.merge(
        mort18.rename(columns={"localidad": "LOCALIDAD_JOIN"}),
        on="LOCALIDAD_JOIN",
        how="left",
    )

    # recalcula tasa si aplica
    if {"casos", "poblacion"}.issubset(panel.columns):
        panel["tasa_x_100k_calc"] = (panel["casos"] / panel["poblacion"]) * 100000

    panel.to_parquet(OUT_PANEL_2018, index=False)
    print(f"OK → {OUT_PANEL_2018} ({len(panel)})")


if __name__ == "__main__":
    main()
