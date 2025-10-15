# ============================
# ETL Bogotá — Makefile
# ============================

# usa un nombre de variable que no quede vacío
PYTHON := python
export PYTHONPATH := .

.PHONY: prepare transform geocode analytics dashboard all clean

# ---------- EXTRACT ----------
prepare:
	@echo "=== 🧩 EXTRACT ==="
	$(PYTHON) -m src.extract.extract_comparendos_2018
	$(PYTHON) -m src.extract.extract_siniestralidad_2018
	$(PYTHON) -m src.extract.extract_semaforos
	$(PYTHON) -m src.extract.extract_mortalidad
	$(PYTHON) -m src.extract.extract_runt
	$(PYTHON) -m src.extract.extract_localidades

# ---------- TRANSFORM ----------
transform:
	@echo "=== ⚙️  TRANSFORM ==="
	@if [ -f "src/transform/geocode_addresses_google_parallel.py" ]; then \
		echo "→ Geocoder: google_parallel"; \
		$(PYTHON) -m src.transform.geocode_addresses_google_parallel; \
	else \
		echo "→ Geocoder: estándar (nominatim)"; \
		$(PYTHON) -m src.transform.geocode_addresses; \
	fi
	$(PYTHON) -m src.transform.join_localidades
	$(PYTHON) -m src.transform.calc_proximidad_semaforos
	$(PYTHON) -m src.transform.agregacion_hex
	$(PYTHON) -m src.transform.merge_mortalidad

# ejecutar solo el geocoder (respeta el fallback)
geocode:
	@if [ -f "src/transform/geocode_addresses_google_parallel.py" ]; then \
		$(PYTHON) -m src.transform.geocode_addresses_google_parallel; \
	else \
		$(PYTHON) -m src.transform.geocode_addresses; \
	fi

# ---------- ANALYTICS ----------
analytics:
	@echo "=== 📊 ANALYTICS ==="
	$(PYTHON) -m src.analytics.resumen_kpi

# ---------- DASHBOARD ----------
dashboard:
	@echo "=== 🖥️  STREAMLIT ==="
	streamlit run src/dashboard/streamlit_app.py

# ---------- PIPELINE COMPLETO ----------
all: prepare transform analytics

# ---------- LIMPIEZA ----------
clean:
	@echo "🧹 Limpiando..."
	rm -rf data/working/*.json data/clean/*.parquet data/analytics/*.parquet data/analytics/*.geojson
	@echo "✅ Ok."
