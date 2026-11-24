#!/usr/bin/env python3

import time
import json
import queue
import logging
import urllib.request
import urllib.parse
import paho.mqtt.client as mqtt
from systemd.journal import JournalHandler
import os
import sys

def require_env(name):
    """Liest eine Pflicht-Umgebungsvariable oder beendet das Skript mit Fehlermeldung."""
    value = os.environ.get(name)
    if not value:
        logger.error(f"Umgebungsvariable {name} fehlt.")
        sys.exit(1)
    return value

# --- Logging ---
logger = logging.getLogger('Oekofen2Mqtt')
logger.addHandler(JournalHandler(SYSLOG_IDENTIFIER="Oekofen2Mqtt"))
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

# --- KONFIGURATION ---
OEKOFEN_HOST = os.environ.get("OEKOFEN_HOST", "http://pelle:4321")
OEKOFEN_PASSWORD = require_env("OEKOFEN_PASSWORD")
OEKOFEN_BASE_URL = f"{OEKOFEN_HOST}/{OEKOFEN_PASSWORD}"
OEKOFEN_URL = f"{OEKOFEN_BASE_URL}/all?"                       # Lesen (mit Metadaten wie factor/min/max)
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mqtt.plattentoni.de")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")                    # leer = keine Authentifizierung
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")
BASE_TOPIC = "oekofen"
STATUS_TOPIC = f"{BASE_TOPIC}/online"                          # LWT: Status des Python-Skripts ("true" / "false")
HEATING_STATUS_TOPIC = f"{BASE_TOPIC}/pelle_online"            # Erreichbarkeit der Heizung ("true" / "false")
COMMAND_SUFFIX = "set"                                         # Command-Topic: oekofen/<gruppe>/<key>/set
COMMAND_TOPIC = f"{BASE_TOPIC}/+/+/{COMMAND_SUFFIX}"
READONLY_PREFIX = "L_"                                         # Keys mit diesem Präfix sind nur lesbar
POLL_INTERVAL = 10                                             # Abfrageintervall in Sekunden
HTTP_TIMEOUT = 5                                               # Timeout für HTTP-Anfragen in Sekunden
REQUEST_GAP = 3                                                # Mindestabstand zwischen zwei API-Anfragen in Sekunden
APPLY_FACTOR = True                                            # True = rechnet z.B. "262" mit factor "0.1" in 26.2 um

# --- Laufzeitdaten ---
writable = {}                   # (gruppe, key) -> JSON-Eintrag (inkl. factor/min/max)
command_queue = queue.Queue()   # Befehle vom MQTT-Thread an den Hauptthread
_last_request = 0.0


def oekofen_request(url):
    """Führt eine HTTP-Anfrage an die Heizung aus und hält den Mindestabstand ein."""
    global _last_request
    wait = REQUEST_GAP - (time.monotonic() - _last_request)
    if wait > 0:
        time.sleep(wait)
    try:
        logger.debug(f"HTTP GET {url}")
        with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as response:
            return response.read().decode("utf-8", errors="replace")
    finally:
        _last_request = time.monotonic()


def on_connect(client, userdata, flags, reason_code, properties):
    """Wird bei jedem (Wieder-)Verbindungsaufbau zum MQTT-Broker aufgerufen."""
    if reason_code == 0:
        logger.info("Mit MQTT-Broker verbunden.")
        client.publish(STATUS_TOPIC, "true", retain=True)
        client.subscribe(COMMAND_TOPIC, qos=1)   # nach Reconnect erneut abonnieren
        logger.info(f"Command-Topic abonniert: {COMMAND_TOPIC}")
    else:
        logger.error(f"MQTT-Verbindungsfehler (Code: {reason_code})")


def on_message(client, userdata, msg):
    """Nimmt Befehle entgegen und reicht sie an den Hauptthread weiter."""
    if msg.retain:
        logger.warning(f"Retained Befehl auf {msg.topic} ignoriert.")
        return

    parts = msg.topic.split("/")
    if len(parts) != 4 or parts[0] != BASE_TOPIC or parts[3] != COMMAND_SUFFIX:
        return

    payload = msg.payload.decode("utf-8", errors="replace").strip()
    command_queue.put((parts[1], parts[2], payload))


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_value(entry):
    """Extrahiert 'val' aus dem JSON-Objekt und wendet 'factor' an."""
    if not isinstance(entry, dict):
        return str(entry)

    raw_val = str(entry.get("val", ""))

    if raw_val.lower() in ("true", "false"):
        return raw_val.lower()

    if APPLY_FACTOR and "factor" in entry:
        try:
            num_val = float(raw_val)
            factor = float(entry["factor"])
            result = round(num_val * factor, 4)
            return str(int(result)) if result.is_integer() else str(result)
        except ValueError:
            pass

    return raw_val


def to_raw_value(entry, value):
    """Wandelt einen MQTT-Wert in den Rohwert der API um und prüft min/max."""
    if value.lower() in ("true", "false"):
        return value.lower()

    if not isinstance(entry, dict) or "factor" not in entry:
        return value   # kein numerischer Wert mit Faktor -> unverändert durchreichen

    num = _to_float(value.replace(",", "."))
    if num is None:
        raise ValueError(f"'{value}' ist keine gültige Zahl")

    factor = _to_float(entry["factor"]) if APPLY_FACTOR else 1.0
    if not factor:
        factor = 1.0
    raw = int(round(num / factor))

    lo, hi = _to_float(entry.get("min")), _to_float(entry.get("max"))
    if lo is not None and raw < lo:
        raise ValueError(f"Rohwert {raw} unterhalb Minimum {entry['min']}")
    if hi is not None and raw > hi:
        raise ValueError(f"Rohwert {raw} oberhalb Maximum {entry['max']}")

    return raw


def handle_command(group, key, payload):
    """Prüft einen Befehl und ruft die passende Schreib-URL auf. Gibt True bei Erfolg zurück."""
    entry = writable.get((group, key))
    if entry is None:
        logger.warning(f"{group}/{key} ist unbekannt oder nicht schreibbar, Befehl '{payload}' ignoriert.")
        return False

    try:
        raw = to_raw_value(entry, payload)
    except ValueError as e:
        logger.error(f"Ungültiger Wert für {group}/{key}: {e}")
        return False

    param = f"{group}_{key}={urllib.parse.quote(str(raw))}"
    url = f"{OEKOFEN_BASE_URL}/{param}"

    try:
        response = oekofen_request(url)
    except Exception as e:
        logger.error(f"Fehler beim Schreiben von {param}: {e}")
        return False

    logger.info(f"Befehl gesendet: {param} (MQTT-Wert: {payload}), Antwort: {response.strip()}")
    return True


def fetch_and_publish(client):
    """Ruft die JSON-Daten ab, sendet sie an MQTT und merkt sich die schreibbaren Werte."""
    global writable

    try:
        data = json.loads(oekofen_request(OEKOFEN_URL))
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Ökofen-Daten: {e}")
        client.publish(HEATING_STATUS_TOPIC, "false", retain=True)
        return

    client.publish(HEATING_STATUS_TOPIC, "true", retain=True)
    published_count = 0
    new_writable = {}

    for group, items in data.items():
        if not isinstance(items, dict):
            continue

        for key, entry in items.items():
            if key.endswith("_info"):
                continue

            topic = f"{BASE_TOPIC}/{group}/{key}"
            client.publish(topic, format_value(entry), retain=True)
            published_count += 1

            if not key.startswith(READONLY_PREFIX):
                new_writable[(group, key)] = entry

    if new_writable.keys() != writable.keys():
        logger.info(f"{len(new_writable)} schreibbare Werte erkannt.")
    writable = new_writable

    logger.info(f"{published_count} Werte erfolgreich an MQTT gesendet.")


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

    client.will_set(STATUS_TOPIC, payload="false", qos=1, retain=True)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=40)
    except Exception as e:
        logger.error(f"Verbindung zum MQTT-Broker fehlgeschlagen: {e}")
        return

    client.loop_start()

    next_poll = 0.0
    try:
        while True:
            timeout = max(0.0, next_poll - time.monotonic())
            try:
                group, key, payload = command_queue.get(timeout=timeout)
            except queue.Empty:
                fetch_and_publish(client)
                next_poll = time.monotonic() + POLL_INTERVAL
                continue

            if handle_command(group, key, payload):
                next_poll = 0.0   # nach erfolgreichem Befehl sofort neu einlesen
    except KeyboardInterrupt:
        logger.info("Skript wird beendet...")
    finally:
        client.publish(STATUS_TOPIC, "false", retain=True)
        time.sleep(0.5)
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
