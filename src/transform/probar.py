import os
from dotenv import load_dotenv
import googlemaps

# Cargar variables del archivo .env
load_dotenv()

# Leer la API key desde el .env
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not API_KEY:
    raise ValueError("❌ No se encontró GOOGLE_MAPS_API_KEY en el archivo .env")

# Inicializar el cliente de Google Maps
gmaps = googlemaps.Client(key=API_KEY)

# Dirección de ejemplo para probar
direccion = "Carrera 7 con Calle 72, Bogotá, Colombia"

# Llamada a la API
resultado = gmaps.geocode(direccion, region="co")

if not resultado:
    print("⚠️ No se obtuvo ningún resultado.")
else:
    data = resultado[0]
    loc = data["geometry"]["location"]
    print("✅ Resultado:")
    print(f"Dirección: {data['formatted_address']}")
    print(f"Latitud: {loc['lat']}")
    print(f"Longitud: {loc['lng']}")
