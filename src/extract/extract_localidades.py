# Uso: python -m src.extract.extract_localidades
# Descarga/lee el GeoJSON de Localidades desde datosabiertos, normaliza nombre y CRS, y guarda en data/raw/localidades/.

import os
import requests
import geopandas as gpd

RAW_DIR = "data/raw/localidades"
os.makedirs(RAW_DIR, exist_ok=True)

URL = "https://datosabiertos.bogota.gov.co/dataset/856cb657-8ca3-4ee8-857f-37211173b1f8/resource/497b8756-0927-4aee-8da9-ca4e32ca3a8a/download/loca.json"
OUT_PATH = os.path.join(RAW_DIR, "localidades.geojson")

def _detect_name_col(cols):
    cands = [c for c in cols if str(c).lower() in (
        "localidad","nombre","locnombre","nom_localidad","nomlocalidad","nombre_localidad","nom_loc"
    )]
    return cands[0] if cands else None

def main():
    # descarga cruda a disco (más reproducible)
    r = requests.get(URL, timeout=90)
    r.raise_for_status()
    with open(OUT_PATH, "wb") as f:
        f.write(r.content)

    gdf = gpd.read_file(OUT_PATH)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    else:
        gdf = gdf.to_crs(4326)

    name_col = _detect_name_col(gdf.columns)
    if name_col and name_col != "LOCALIDAD":
        gdf = gdf.rename(columns={name_col: "LOCALIDAD"})
    elif "LOCALIDAD" not in gdf.columns:
        # si no hay nombre identificable, crea uno genérico incremental
        gdf["LOCALIDAD"] = [f"LOC_{i+1:02d}" for i in range(len(gdf))]

    gdf["LOCALIDAD"] = gdf["LOCALIDAD"].astype(str).str.strip().str.upper()

    # área km² (útil para densidades)
    gdf_m = gdf.to_crs(3857)
    gdf["AREA_KM2"] = gdf_m.geometry.area / 1e6

    gdf.to_file(OUT_PATH, driver="GeoJSON")
    print(f"OK localidades → {OUT_PATH} ({len(gdf)} polígonos)")

if __name__ == "__main__":
    main()
