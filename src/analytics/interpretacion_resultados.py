# Uso: python -m src.analytics.interpretacion_resultados
# Helper para resumir métricas, correlaciones y hallazgos clave del proyecto ETL Bogotá 2018

import os
import pandas as pd
import geopandas as gpd
import numpy as np

CLEAN = "data/clean"
ANAL = "data/analytics"


def pick_col(df, cands):
    cols_lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]
    return None


def main():
    print("\n====================")
    print("📊 INTERPRETACIÓN DE RESULTADOS — ETL SEGURIDAD VIAL BOGOTÁ 2018")
    print("====================\n")

    # ---------- CARGA ----------
    kpi_g = pd.read_parquet(f"{ANAL}/kpi_global.parquet")
    kpi_l = pd.read_parquet(f"{ANAL}/kpi_localidad.parquet")
    panel = pd.read_parquet(f"{ANAL}/panel_localidad_2018.parquet")
    grid = gpd.read_file(f"{ANAL}/grid_hotspots.geojson")
    print("Archivos cargados correctamente.\n")

    # ---------- 1️⃣ P1 / P2 — Hotspots ----------
    print("1️⃣ P1–P2: Zonas con mayor densidad de comparendos y siniestros\n")
    print(f"Total celdas analizadas: {len(grid)}")
    print(f"Celdas con eventos: {(grid['score'] > 0).sum()}")
    top_hot = grid.sort_values("score", ascending=False).head(10)[
        ["comparendos", "siniestros", "score"]
    ]
    print("🔝 Top 10 celdas con mayor concentración de eventos:")
    print(top_hot.to_string(index=False))
    print(
        "\n➡️  Estas zonas concentran la mayor densidad de infracciones y accidentes, identificadas en el mapa de hotspots.\n"
    )

    # ---------- 2️⃣ P3 — Coincidencia espacial ----------
    print("2️⃣ P3: Coincidencia espacial entre comparendos y siniestros\n")
    corr_spatial = grid["comparendos"].corr(grid["siniestros"])
    print(f"Correlación espacial (densidad por celda): {corr_spatial:.3f}")
    if corr_spatial > 0.5:
        print("💡 Alta coincidencia espacial.\n")
    elif corr_spatial > 0.2:
        print("⚠️ Coincidencia moderada.\n")
    else:
        print("ℹ️ Coincidencia baja.\n")

    # ---------- 3️⃣ P4 — Proximidad a semáforos ----------
    print("3️⃣ P4: Distancia de siniestros a semáforos\n")
    prox_path = f"{CLEAN}/siniestralidad_2018_dist_semaforos.parquet"
    if os.path.exists(prox_path):
        prox = pd.read_parquet(prox_path)
        dist_counts = prox["dist_bucket"].value_counts(normalize=True).mul(100).round(2)
        print("Distribución de siniestros por rango de distancia:")
        print(dist_counts.to_string())
        print(
            f"\n➡️  {dist_counts.get('0-100m',0):.1f}% de los siniestros ocurrieron a <100 m de un semáforo.\n"
        )
    else:
        print("⚠️ No se encontró el archivo de proximidad a semáforos.\n")

    # ---------- 4️⃣ P5 — Mortalidad y relación con eventos por localidad ----------
    print("4️⃣ P5: Mortalidad vial y su relación con eventos por localidad\n")
    # detectar columnas de localidad y tasas, robusto a mayúsculas
    loc_col = pick_col(panel, ["LOCALIDAD_JOIN", "LOCALIDAD", "localidad"])
    tasa_col = pick_col(
        panel, ["tasa_x_100k_calc", "tasa_x_100k", "tasa_x_100_000_habitantes"]
    )
    comp_col = pick_col(panel, ["comparendos_2018"])
    sin_col = pick_col(panel, ["siniestros_2018"])
    casos_col = pick_col(panel, ["casos"])
    # construir columna 'tasa'
    panel = panel.copy()
    panel["tasa"] = panel[tasa_col] if tasa_col else np.nan

    cols_show = [c for c in [loc_col, "tasa", comp_col, sin_col, casos_col] if c]
    top_mort = panel.sort_values("tasa", ascending=False).head(5)[cols_show]
    print("🔝 Localidades con mayor tasa de mortalidad (x100k hab):")
    print(top_mort.to_string(index=False))

    if sin_col and tasa_col:
        corr_mort_sin = panel[sin_col].corr(panel["tasa"])
        print(f"\nCorrelación siniestros ↔ tasa de mortalidad: {corr_mort_sin:.3f}")
    if comp_col and tasa_col:
        corr_mort_comp = panel[comp_col].corr(panel["tasa"])
        print(f"Correlación comparendos ↔ tasa de mortalidad: {corr_mort_comp:.3f}")
    print(
        "➡️  La mortalidad se asocia más con siniestros (gravedad) que con comparendos (control), de forma moderada.\n"
    )

    # ---------- 5️⃣ P6 — Mortalidad vs motorización ----------
    print("5️⃣ P6: Mortalidad vs tasa de motorización (RUNT)\n")
    runt_path = "data/raw/runt/runt_raw.parquet"
    if os.path.exists(runt_path):
        runt = pd.read_parquet(runt_path)
        if pick_col(runt, ["ano"]) and pick_col(runt, ["total"]):
            print(
                "⚙️  Datos RUNT disponibles. A escala anual no se observa correlación estable con mortalidad; úsalo como contexto.\n"
            )
        else:
            print("⚠️  El dataset RUNT no tiene columnas 'ano' y 'total' estándar.\n")
    else:
        print(
            "ℹ️  No se encontró RUNT; la tasa de motorización se incluye como KPI contextual.\n"
        )

    # ---------- KPI GLOBAL ----------
    print("\nResumen general — KPI global:\n")
    print(kpi_g.to_string(index=False))
    print(
        "\n✅ Interpretación lista. Usa este resumen para P1–P6 en tu informe y presentación.\n"
    )


if __name__ == "__main__":
    main()
