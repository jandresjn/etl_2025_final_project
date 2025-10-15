# ETL Bogotá – Seguridad Vial 2018
Pipeline ETL para integrar comparendos 2018, siniestralidad 2018 (geocodificada), red semafórica y localidades, con dashboard Streamlit (mapa + KPIs).

## Integrantes
- Jorge Andres Jaramillo Neme
- Luis Fernando Meza

## Preguntas
1) Hotspots de comparendos 2018  
2) Hotspots de siniestros 2018  
3) Coincidencia espacial comparendos–siniestros  
4) Proximidad de siniestros a semáforos  
5) Densidad y mortalidad por localidad

## Ejecutar
```bash
python -m pip install -r requirements.txt
make all
make dashboard
