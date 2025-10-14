# Uso: python -m src.transform.merge_mortalidad
import os, pandas as pd

CLEAN, ANAL = "data/clean", "data/analytics"
os.makedirs(ANAL, exist_ok=True)

SRC = "data/raw/mortalidad_localidad.csv"  # coloca tu CSV aquí (Año, Localidad, Casos, Población, Tasa...)


def main():
    m = pd.read_csv(SRC)
    # Buenas prácticas: nombres consistentes y año 2018
    cols = {c: c.lower().strip().replace(" ", "_") for c in m.columns}
    m = m.rename(columns=cols)
    m["localidad"] = m["localidad"].astype(str).str.upper().str.strip()
    m = m[m["ano"].astype(str).eq("2018")]
    m = m[["localidad", "casos", "poblacion", "tasa_x_100_000_habitantes"]]
    m.to_parquet(f"{CLEAN}/mortalidad_2018_localidad.parquet", index=False)
    print("OK → data/clean/mortalidad_2018_localidad.parquet")


if __name__ == "__main__":
    main()
