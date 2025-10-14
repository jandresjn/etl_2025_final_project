# Uso: python -m src.analytics.resumen_kpi
import os, pandas as pd, geopandas as gpd

CLEAN, ANAL = "data/clean", "data/analytics"
os.makedirs(ANAL, exist_ok=True)


def main():
    comp = pd.read_parquet(f"{CLEAN}/comparendos_2018_loc.parquet")
    sin = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_loc.parquet")
    prox = pd.read_parquet(f"{CLEAN}/siniestralidad_2018_proximidad_semaforos.parquet")
    mort = pd.read_parquet(f"{CLEAN}/mortalidad_2018_localidad.parquet")

    # KPIs globales
    kpi = {
        "comparendos_total": len(comp),
        "siniestros_total": len(sin),
        "prox_<100m": prox["dist_bucket"].eq("<100 m").mean(),
        "prox_100_300": prox["dist_bucket"].eq("100–300 m").mean(),
        "prox_>300": prox["dist_bucket"].eq(">300 m").mean(),
    }
    pd.DataFrame([kpi]).to_parquet(f"{ANAL}/kpi_global.parquet", index=False)

    # Por localidad (usa LOCALIDAD_JOIN si existe, fallback LOCALIDAD)
    loc_col = "LOCALIDAD_JOIN" if "LOCALIDAD_JOIN" in comp.columns else "LOCALIDAD"
    c_loc = (
        comp.groupby(loc_col)
        .size()
        .rename("comparendos")
        .reset_index()
        .rename(columns={loc_col: "localidad"})
    )
    s_loc = (
        sin.groupby(loc_col)
        .size()
        .rename("siniestros")
        .reset_index()
        .rename(columns={loc_col: "localidad"})
    )

    res = c_loc.merge(s_loc, on="localidad", how="outer").fillna(0)
    res = res.merge(mort, on="localidad", how="left")
    res.to_parquet(f"{ANAL}/kpi_localidad.parquet", index=False)
    print("OK → data/analytics/kpi_*.parquet")


if __name__ == "__main__":
    main()
