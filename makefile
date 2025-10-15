PY=python

prepare:
	$(PY) -m src.extract.extract_comparendos_2018
	$(PY) -m src.extract.extract_siniestralidad_2018
	$(PY) -m src.extract.extract_semaforos
	$(PY) -m src.extract.extract_mortalidad
	$(PY) -m src.extract.extract_runt
	$(PY) -m src.extract.extract_localidades

transform:
	$(PY) -m src.transform.geocode_addresses
	$(PY) -m src.transform.join_localidades
	$(PY) -m src.transform.agregacion_hex
	$(PY) -m src.transform.calc_proximidad_semaforos
	$(PY) -m src.transform.merge_mortalidad

analytics:
	$(PY) -m src.analytics.resumen_kpi

dashboard:
	streamlit run src/dashboard/streamlit_app.py

all: prepare transform analytics
