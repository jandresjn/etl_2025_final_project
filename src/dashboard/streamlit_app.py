import streamlit as st
import pandas as pd
import geopandas as gpd
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap
import plotly.express as px

st.set_page_config(page_title="Seguridad Vial Bogotá 2018", layout="wide")


@st.cache_data
def load_data():
    kpi_g = pd.read_parquet("data/analytics/kpi_global.parquet")
    kpi_l = pd.read_parquet("data/analytics/kpi_localidad.parquet")
    grid = gpd.read_file("data/analytics/grid_hotspots.geojson")
    comp = pd.read_parquet("data/clean/comparendos_2018_loc.parquet")
    sin = pd.read_parquet("data/clean/siniestralidad_2018_loc.parquet")
    sem = pd.read_parquet("data/raw/semaforos.parquet")
    return kpi_g, kpi_l, grid, comp, sin, sem


kpi_g, kpi_l, grid, comp, sin, sem = load_data()

st.title("Seguridad Vial Bogotá 2018 — ETL + Dashboard")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Comparendos", f"{int(kpi_g['comparendos_total'].iloc[0]):,}")
c2.metric("Siniestros", f"{int(kpi_g['siniestros_total'].iloc[0]):,}")
c3.metric("% siniestros <100m de semáforo", f"{kpi_g['prox_<100m'].iloc[0]*100:.1f}%")
c4.metric("% siniestros >300m", f"{kpi_g['prox_>300'].iloc[0]*100:.1f}%")

st.subheader("Mapa interactivo")
with st.container():
    m = folium.Map(location=[4.65, -74.1], zoom_start=11, tiles="CartoDB positron")

    # Heatmap comparendos
    HeatMap(
        comp[["lat", "lon"]].dropna().values.tolist(),
        radius=7,
        blur=15,
        name="Heatmap comparendos",
    ).add_to(m)

    # Puntos siniestros (cluster liviano: opcional)
    for _, r in sin.sample(min(3000, len(sin))).iterrows():  # muestra para performance
        folium.CircleMarker(
            [r["lat"], r["lon"]], radius=2, color="#d62728", fill=True, fill_opacity=0.6
        ).add_to(m)

    # Semáforos
    for _, r in sem.sample(min(2000, len(sem))).iterrows():
        folium.CircleMarker(
            [r["lat"], r["lon"]], radius=1, color="#ffbf00", fill=True, fill_opacity=0.8
        ).add_to(m)

    # Grid hotspots (transparencia)
    folium.GeoJson(
        grid.to_json(),
        name="Hotspots",
        style_function=lambda x: {
            "fillColor": "#000",
            "color": "#000",
            "weight": 0.2,
            "fillOpacity": min(
                0.6,
                (
                    x["properties"].get("score_hotspot", 0)
                    / grid["score_hotspot"].max()
                    + 1e-6
                ),
            ),
        },
    ).add_to(m)

    folium.LayerControl().add_to(m)
    st_folium(m, height=640, width=None)

st.subheader("Localidades — comparendos, siniestros y mortalidad")
kpi_l_sorted = kpi_l.sort_values("siniestros", ascending=False)
fig = px.bar(
    kpi_l_sorted,
    x="localidad",
    y=["comparendos", "siniestros"],
    barmode="group",
    title="Eventos por localidad",
)
st.plotly_chart(fig, use_container_width=True)

if "tasa_x_100_000_habitantes" in kpi_l.columns:
    fig2 = px.bar(
        kpi_l.sort_values("tasa_x_100_000_habitantes", ascending=False),
        x="localidad",
        y="tasa_x_100_000_habitantes",
        title="Tasa de mortalidad (x100k hab)",
    )
    st.plotly_chart(fig2, use_container_width=True)

st.caption(
    "Datos: Comparendos 2018 (ArcGIS), Anuario Siniestralidad 2018, Red Semafórica, Localidades (GeoJSON)."
)
