# Uso: python -m src.analytics.resumen_kpi
# Lee outputs de transform y genera KPIs globales y por localidad.

import os
import pandas as pd

CLEAN, ANAL = "data/clean", "data/analytics"
os.makedirs(ANAL, exist_ok=True)


def _pick_loc_col(df):
    cands = [c for c in df.columns if "LOCALIDAD" in c.upper()]
    if not cands:
        return None
    cands = sorted(
        cands,
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
    return cands[0]


def main():
    comp = pd.read_parquet(f"{CLEAN}/comparendos_2018_loc.parquet")
    sin = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_loc.parquet")
    prox = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_dist_semaforos.parquet")
    panel = pd.read_parquet(
        f"{ANAL}/panel_localidad_2018.parquet"
    )  # contiene mortalidad 2018

    # KPIs globales
    buckets = prox["dist_bucket"].value_counts(normalize=True)
    kpi = {
        "comparendos_total": int(len(comp)),
        "siniestros_total": int(len(sin)),
        "prox_0_100_pct": float(buckets.get("0-100m", 0.0)),
        "prox_100_300_pct": float(buckets.get("100-300m", 0.0)),
        "prox_mayor_300_pct": float(buckets.get(">300m", 0.0)),
    }
    pd.DataFrame([kpi]).to_parquet(f"{ANAL}/kpi_global.parquet", index=False)

    # Por localidad
    comp_loc_col = _pick_loc_col(comp) or "LOCALIDAD"
    sin_loc_col = _pick_loc_col(sin) or "LOCALIDAD"

    c_loc = (
        comp.groupby(comp_loc_col)
        .size()
        .rename("comparendos_2018")
        .reset_index()
        .rename(columns={comp_loc_col: "LOCALIDAD"})
    )
    s_loc = (
        sin.groupby(sin_loc_col)
        .size()
        .rename("siniestros_2018")
        .reset_index()
        .rename(columns={sin_loc_col: "LOCALIDAD"})
    )

    # Panel ya trae: LOCALIDAD_JOIN, comparendos_2018, siniestros_2018, casos, poblacion, tasa_x_100k, tasa_x_100k_calc
    # Para homogenizar, renombra LOCALIDAD_JOIN -> LOCALIDAD
    panel_loc = panel.rename(columns={"LOCALIDAD_JOIN": "LOCALIDAD"})

    # Unificación por LOCALIDAD
    kpi_loc = (
        panel_loc[
            [
                "LOCALIDAD",
                "comparendos_2018",
                "siniestros_2018",
                "casos",
                "poblacion",
                "tasa_x_100k",
                "tasa_x_100k_calc",
            ]
        ]
        .merge(c_loc, on="LOCALIDAD", how="outer")
        .merge(s_loc, on="LOCALIDAD", how="outer")
    )

    # Rellenos y tipos
    for col in [
        "comparendos_2018_x",
        "siniestros_2018_x",
        "comparendos_2018_y",
        "siniestros_2018_y",
    ]:
        if col in kpi_loc.columns:
            kpi_loc[col] = kpi_loc[col].fillna(0).astype("Int64")

    # Consolidación (prefiere los del panel y usa los del conteo directo si faltan)
    kpi_loc["comparendos_2018_final"] = (
        kpi_loc.get("comparendos_2018_x", pd.Series(dtype="Int64"))
        .fillna(kpi_loc.get("comparendos_2018_y", pd.Series(dtype="Int64")))
        .astype("Int64")
    )
    kpi_loc["siniestros_2018_final"] = (
        kpi_loc.get("siniestros_2018_x", pd.Series(dtype="Int64"))
        .fillna(kpi_loc.get("siniestros_2018_y", pd.Series(dtype="Int64")))
        .astype("Int64")
    )

    cols_final = [
        "LOCALIDAD",
        "comparendos_2018_final",
        "siniestros_2018_final",
        "casos",
        "poblacion",
        "tasa_x_100k",
        "tasa_x_100k_calc",
    ]
    kpi_loc = kpi_loc[cols_final].rename(
        columns={
            "comparendos_2018_final": "comparendos_2018",
            "siniestros_2018_final": "siniestros_2018",
        }
    )

    kpi_loc.to_parquet(f"{ANAL}/kpi_localidad.parquet", index=False)
    print("OK → data/analytics/kpi_global.parquet, kpi_localidad.parquet")


if __name__ == "__main__":
    main()
