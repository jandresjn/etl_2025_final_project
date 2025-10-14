# Uso: python -m src.extract.extract_localidades
import os, geopandas as gpd

RAW = "data/raw"
os.makedirs(RAW, exist_ok=True)

# GeoJSON de localidades (ajusta ruta/URL real)
SOURCE = "data/raw/localidades.geojson"


def main():
    gdf = gpd.read_file(SOURCE)
    gdf = gdf.to_crs(4326)
    gdf.to_file("data/raw/localidades_4326.geojson", driver="GeoJSON")
    print("OK:", len(gdf), "polygons → data/raw/localidades_4326.geojson")


if __name__ == "__main__":
    main()
