import sys
import time
import json
import random

import requests
from kafka import KafkaProducer
from Constantes.constantes import Constantes

TOPIC = Constantes.KAFKA_TOPIC_NAME
BOOTSTRAP = [Constantes.PORT]
URL = Constantes.API_URL

# Zona: centro de Colombia (Bogota, Medellin, Cali). Un area de hasta 25 grados cuadrados
# cuesta 1 credito por consulta; areas mayores cuestan mas (el acceso anonimo tiene 400 al dia).
ZONA = {"lamin": 3.0, "lomin": -77.5, "lamax": 7.5, "lomax": -73.0}
INTERVALO = 10

SIMULADO = "--simulado" in sys.argv

def estados_opensky():
    """Consulta la API y devuelve (lista de eventos, creditos restantes)."""
    r = requests.get(URL, params=ZONA, timeout=10)
    if r.status_code == 429:
        espera = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 60))
        raise RuntimeError(f"Limite de la API alcanzado (429). Reintentar en {espera} s")
    r.raise_for_status()
    datos = r.json()
    restantes = r.headers.get("X-Rate-Limit-Remaining")

    eventos = []
    for s in datos.get("states") or []:
        # s: [0]icao24 [1]callsign [2]pais_origen [5]lon [6]lat [7]altitud_baro
        #    [8]en_tierra [9]velocidad (m/s)
        if s[5] is None or s[6] is None:
            continue  # sin posicion no sirve para el analisis
        eventos.append({
            "icao24": s[0],
            "callsign": (s[1] or "").strip(),
            "pais": s[2],
            "lon": s[5],
            "lat": s[6],
            "altitud_m": s[7],
            "velocidad_kmh": round(s[9] * 3.6, 2) if s[9] is not None else None,
            "en_tierra": bool(s[8]),
            "timestamp": datos["time"],
        })
    return eventos, restantes


def estados_simulados():
    """Genera aeronaves aleatorias con el mismo formato que la API (para pruebas)."""
    paises = ["Colombia", "United States", "Chile", "Panama", "Peru", "Mexico", "Spain"]
    ahora = int(time.time())
    eventos = []
    for i in range(random.randint(30, 50)):
        eventos.append({
            "icao24": f"sim{i:03d}",
            "callsign": f"SIM{random.randint(100, 999)}",
            "pais": random.choice(paises),
            "lon": round(random.uniform(ZONA["lomin"], ZONA["lomax"]), 4),
            "lat": round(random.uniform(ZONA["lamin"], ZONA["lamax"]), 4),
            "altitud_m": round(random.uniform(2000, 12000), 1),
            "velocidad_kmh": round(random.uniform(300, 900), 2),
            "en_tierra": False,
            "timestamp": ahora,
        })
    return eventos, "n/a"


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

print(f"Productor iniciado | topic={TOPIC} | modo={'SIMULADO' if SIMULADO else 'API OpenSky'}")
try:
    while True:
        try:
            eventos, restantes = estados_simulados() if SIMULADO else estados_opensky()
        except Exception as e:  # red caida, limite de la API, etc.
            print(f"Error al obtener datos: {e}. Reintentando en 30 s")
            time.sleep(30)
            continue

        for evento in eventos:
            producer.send(TOPIC, value=evento)
        producer.flush()

        print(f"Sent: {len(eventos)} eventos | creditos restantes: {restantes} | ejemplo: {eventos[0] if eventos else '-'}")
        time.sleep(INTERVALO)
except KeyboardInterrupt:
    print("Productor detenido")
finally:
    producer.close()
