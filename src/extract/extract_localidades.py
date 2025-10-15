# Uso: python -m src.extract.extract_localidades
# Carga polígonos de localidades (GeoJSON oficial)
import os, geopandas as gpd

RAW = "data/raw/localidades"
os.makedirs(RAW, exist_ok=True)

URL = "https://opendata.arcgis.com/datasets/8258cb6db7a04d3c86ad02e65ef2e8ed_0.geojson"  # localidades Bogotá


def main():
    gdf = gpd.read_file(URL)
    gdf = gdf.to_crs(4326)
    gdf.to_file(f"{RAW}/localidades.geojson", driver="GeoJSON")
    print("OK localidades → data/raw/localidades/localidades.geojson", len(gdf))


if __name__ == "__main__":
    main()
