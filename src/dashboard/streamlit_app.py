import os
import pandas as pd
import geopandas as gpd
import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap
import plotly.express as px

st.set_page_config(page_title="Seguridad Vial Bogotá 2018", layout="wide")


# ------------------------- Carga -------------------------
@st.cache_data
def load_data():
    kpi_g = pd.read_parquet("data/analytics/kpi_global.parquet")
    kpi_loc = pd.read_parquet("data/analytics/kpi_localidad.parquet")
    grid = gpd.read_file("data/analytics/grid_hotspots.geojson")
    comp = pd.read_parquet("data/clean/comparendos_2018_loc.parquet")
    sin = pd.read_parquet("data/clean/siniestralidad_2018_loc.parquet")
    prox = pd.read_parquet("data/clean/siniestralidad_2018_dist_semaforos.parquet")
    sem = pd.read_parquet("data/raw/semaforos/semaforos_raw.parquet")
    return kpi_g, kpi_loc, grid, comp, sin, prox, sem


kpi_g, kpi_loc, grid, comp, sin, prox, sem = load_data()

# Normalizaciones livianas
if "LOCALIDAD" in kpi_loc.columns:
    kpi_loc["localidad_plot"] = kpi_loc["LOCALIDAD"].str.title()
elif "localidad" in kpi_loc.columns:
    kpi_loc["localidad_plot"] = kpi_loc["localidad"].astype(str).str.title()
else:
    kpi_loc["localidad_plot"] = "Sin localidad"

# ------------------------- Sidebar -------------------------
st.sidebar.header("Filtros")
show_heat_comp = st.sidebar.checkbox("Heatmap de comparendos", value=True)
show_pts_sin = st.sidebar.checkbox("Puntos de siniestros (muestra)", value=True)
show_sem = st.sidebar.checkbox("Semáforos", value=False)
grid_opacity = st.sidebar.slider("Opacidad hotspots", 0.1, 0.9, 0.45, 0.05)
heat_radius = st.sidebar.slider("Radio heatmap", 3, 20, 8)
heat_blur = st.sidebar.slider("Blur heatmap", 5, 30, 15)
sample_sin = st.sidebar.slider("Muestra de siniestros (puntos)", 500, 5000, 2000, 100)
sample_sem = st.sidebar.slider("Muestra de semáforos", 500, 8000, 2000, 100)

# ------------------------- KPIs -------------------------
st.title("Seguridad Vial Bogotá 2018 — ETL + Dashboard")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Comparendos", f"{int(kpi_g['comparendos_total'].iloc[0]):,}")
c2.metric("Siniestros", f"{int(kpi_g['siniestros_total'].iloc[0]):,}")
c3.metric(
    "% siniestros a 0–100 m de semáforo", f"{kpi_g['prox_0_100_pct'].iloc[0]*100:.1f}%"
)
c4.metric(
    "% siniestros a >300 m de semáforo",
    f"{kpi_g['prox_mayor_300_pct'].iloc[0]*100:.1f}%",
)

# ------------------------- Mapa -------------------------
st.subheader("Mapa de hotspots y capas")

with st.container():
    m = folium.Map(location=[4.65, -74.1], zoom_start=11, tiles="CartoDB positron")

    # Heatmap comparendos
    if show_heat_comp and {"lat", "lon"}.issubset(comp.columns):
        HeatMap(
            comp[["lat", "lon"]].dropna().values.tolist(),
            radius=heat_radius,
            blur=heat_blur,
            name="Heatmap comparendos",
        ).add_to(m)

    # Puntos siniestros (muestra)
    if show_pts_sin and {"lat", "lon"}.issubset(sin.columns) and len(sin) > 0:
        layer_sin = folium.FeatureGroup(name="Siniestros (muestra)", show=True)
        for _, r in sin.sample(min(sample_sin, len(sin))).iterrows():
            folium.CircleMarker(
                [r["lat"], r["lon"]],
                radius=2,
                color="#d62728",
                fill=True,
                fill_opacity=0.6,
            ).add_to(layer_sin)
        layer_sin.add_to(m)

    # Semáforos (muestra)
    if show_sem and {"lat", "lon"}.issubset(sem.columns) and len(sem) > 0:
        layer_sem = folium.FeatureGroup(name="Semáforos (muestra)", show=True)
        for _, r in sem.sample(min(sample_sem, len(sem))).iterrows():
            folium.CircleMarker(
                [r["lat"], r["lon"]],
                radius=1.5,
                color="#ffbf00",
                fill=True,
                fill_opacity=0.9,
            ).add_to(layer_sem)
        layer_sem.add_to(m)

    # Grid hotspots
    if "score" in grid.columns:
        max_score = max(grid["score"].max(), 1)
        folium.GeoJson(
            grid.to_json(),
            name="Hotspots (500 m)",
            style_function=lambda x: {
                "fillColor": "#000000",
                "color": "#000000",
                "weight": 0.2,
                "fillOpacity": min(
                    0.9,
                    (x["properties"].get("score", 0) / max_score) * grid_opacity + 1e-6,
                ),
            },
            highlight_function=lambda x: {"weight": 1, "color": "#666666"},
            tooltip=folium.GeoJsonTooltip(
                fields=["comparendos", "siniestros", "score"],
                aliases=["Comparendos", "Siniestros", "Score"],
                sticky=False,
            ),
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    st_folium(m, height=640, width=None)

# ------------------------- Barras por localidad -------------------------
st.subheader("Comparendos, siniestros y mortalidad por localidad (2018)")


# Inferir nombres de columnas en tu kpi_localidad
def pick_col(cands):
    for c in cands:
        if c in kpi_loc.columns:
            return c
    return None


col_loc = pick_col(["LOCALIDAD", "localidad"])
col_comp = pick_col(["comparendos_2018", "comparendos"])
col_sin = pick_col(["siniestros_2018", "siniestros"])
col_tasa = pick_col(["tasa_x_100k", "tasa_x_100_000_habitantes", "tasa_x_100k_calc"])

plot_df = kpi_loc.copy()
plot_df["localidad_plot"] = plot_df.get(
    "localidad_plot", plot_df.get(col_loc, "Localidad")
)

left, right = st.columns([2, 1], gap="large")

with left:
    if col_comp and col_sin and col_loc:
        top_by = st.selectbox("Ordenar por", ["siniestros", "comparendos"], index=0)
        if top_by == "siniestros":
            plot_df_ord = plot_df.sort_values(col_sin, ascending=False)
        else:
            plot_df_ord = plot_df.sort_values(col_comp, ascending=False)

        fig = px.bar(
            plot_df_ord,
            x="localidad_plot",
            y=[col_comp, col_sin],
            barmode="group",
            title="Eventos por localidad",
            labels={
                "value": "Eventos",
                "localidad_plot": "Localidad",
                "variable": "Tipo",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    if col_tasa and col_loc:
        plot_df_tasa = plot_df.dropna(subset=[col_tasa]).sort_values(
            col_tasa, ascending=False
        )
        fig2 = px.bar(
            plot_df_tasa,
            x="localidad_plot",
            y=col_tasa,
            title="Tasa de mortalidad (x 100.000 hab.)",
            labels={col_tasa: "Tasa (x100k)", "localidad_plot": "Localidad"},
        )
        st.plotly_chart(fig2, use_container_width=True)

# ------------------------- Distancias a semáforos -------------------------
st.subheader("Distribución de siniestros por distancia al semáforo más cercano")
if "dist_bucket" in prox.columns:
    # Renombrar a etiquetas legibles
    map_labels = {
        "0-100m": "0–100 m",
        "100-300m": "100–300 m",
        ">300m": ">300 m",
        "SIN_COORD": "Sin coord.",
    }
    prox["_bucket_lbl"] = (
        prox["dist_bucket"].map(map_labels).fillna(prox["dist_bucket"])
    )
    dist_counts = (
        prox["_bucket_lbl"]
        .value_counts(normalize=True)
        .rename("pct")
        .mul(100)
        .reset_index()
    )
    dist_counts.columns = ["bucket", "pct"]

    fig3 = px.bar(
        dist_counts.sort_values("bucket"),
        x="bucket",
        y="pct",
        text=dist_counts["pct"].map(lambda x: f"{x:.1f}%"),
        labels={"bucket": "Rango", "pct": "% siniestros"},
        title="Siniestros por distancia al semáforo",
    )
    st.plotly_chart(fig3, use_container_width=True)

# ------------------------- Footer -------------------------
st.caption(
    "Datos: Comparendos 2018 (ArcGIS), Anuario de Siniestralidad 2018, Red Semafórica (SIMUR), Localidades (GeoJSON), "
    "Mortalidad (OSB). © Proyecto ETL 2025-2."
)
