import pandas as pd
import json
import re
import os
from datetime import datetime, timezone
from rapidfuzz import process

bundeslaender = {
    "BW": "Baden-Württemberg",
    "BY": "Bayern",
    "BE": "Berlin",
    "BB": "Brandenburg",
    "HB": "Bremen",
    "HH": "Hamburg",
    "HE": "Hessen",
    "MV": "Mecklenburg-Vorpommern",
    "NI": "Niedersachsen",
    "NW": "Nordrhein-Westfalen",
    "RP": "Rheinland-Pfalz",
    "SL": "Saarland",
    "SN": "Sachsen",
    "ST": "Sachsen-Anhalt",
    "SH": "Schleswig-Holstein",
    "TH": "Thüringen"
}


base_url = "https://www.dwd.de/DWD/warnungen/agrar/wbx/wbx_tab_alle_{kuerzel}.html"

# Heuristik: Die DWD-Waldbrandindex-Tabellen sind saisonal (typisch April–September).
# Wir behandeln Oktober–März als Off-Season und erwarten ggf. 404.
OFFSEASON_MONTHS = {10, 11, 12, 1, 2, 3}
run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")


fetch_ok = 0
fetch_404 = 0
fetch_other_err = 0

for kuerzel, name in bundeslaender.items():
    url = base_url.format(kuerzel=kuerzel)
    outfile = f"waldbrand_{kuerzel}.json"
    try:
        dfs = pd.read_html(url)
        print(f"✅ {name} ({kuerzel}): {len(dfs)} Tabellen gefunden")
        if len(dfs) >= 2:
            df = dfs[1]
            # Filtere die Legenden-Zeile heraus (die eine Wiederholung der Spaltennamen ist)
            df = df[df[df.columns[0]] != df.columns[0]]
            df_json = df.to_dict(orient="records")
            with open(outfile, "w", encoding="utf-8") as f:
                json.dump(df_json, f, ensure_ascii=False, indent=2)
            fetch_ok += 1
            print(f"💾 Datei gespeichert: {outfile}")
        else:
            print(f"⚠️ {name}: Zu wenige Tabellen")
    except Exception as e:
        msg = str(e)
        # In der Off-Season liefert DWD häufig 404. Dann behalten wir die zuletzt bekannten Dateien bei.
        if "HTTP Error 404" in msg or "404" in msg:
            fetch_404 += 1
            if os.path.exists(outfile):
                print(f"🧊 Off-Season/404 für {name} ({kuerzel}) – verwende bestehende Datei: {outfile}")
            else:
                print(f"🧊 Off-Season/404 für {name} ({kuerzel}) – keine bestehende Datei vorhanden")
        else:
            fetch_other_err += 1
            print(f"❌ Fehler bei {name} ({kuerzel}): {e}")


# Alle einzelnen JSON-Dateien zusammenführen

gesamt_daten = []

meta = {
    "run_timestamp_utc": run_ts,
    "source": "DWD wbx_tab_alle_{BL}.html",
    "fetch_ok": fetch_ok,
    "fetch_404": fetch_404,
    "fetch_other_err": fetch_other_err,
    "offseason_expected": datetime.now().month in OFFSEASON_MONTHS,
    "data_status": "unknown"
}

for kuerzel in bundeslaender.keys():
    dateiname = f"waldbrand_{kuerzel}.json"
    if os.path.exists(dateiname):
        with open(dateiname, encoding="utf-8") as f:
            eintraege = json.load(f)
            for eintrag in eintraege:
                eintrag["Bundesland"] = bundeslaender[kuerzel]
            gesamt_daten.extend(eintraege)

# Status bestimmen: wenn alle Abrufe 404 waren (oder Off-Season), markieren wir Daten als "stale/offseason".
if fetch_ok == 0 and fetch_404 > 0:
    meta["data_status"] = "offseason_or_unavailable"
else:
    meta["data_status"] = "fresh_or_partial"

# Meta-Datei schreiben (für die App/UI hilfreich)
with open("waldbrand_meta.json", "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)
print(f"🧾 Meta gespeichert: waldbrand_meta.json ({meta['data_status']})")

# Nach dem Einlesen von gesamt_daten: Doppelte normalisierte Stationsnamen identifizieren
from collections import Counter

def normalize_name(name):
    name = name.upper()
    name = name.replace("Ä", "AE").replace("Ö", "OE").replace("Ü", "UE")
    name = name.replace("ä", "AE").replace("ö", "OE").replace("ü", "UE")
    name = name.replace("ß", "SS")
    name = re.sub(r"[^\w\s]", " ", name)  # Entferne Sonderzeichen wie . und - und ersetze sie durch Leerzeichen
    name = re.sub(r"\s+", " ", name)  # Mehrfache Leerzeichen zusammenfassen
    return name.strip()

counter = Counter([normalize_name(e.get("Stationsname", "")) for e in gesamt_daten])
mehrfach_namen = {k: v for k, v in counter.items() if v > 1}
print(f"👀 Mehrfache Stationsnamen (normalisiert): {mehrfach_namen}")

# Gesamtdatei schreiben (nur überschreiben, wenn wir überhaupt Daten haben)
if len(gesamt_daten) == 0:
    print("⚠️ Keine Gesamtdaten vorhanden – waldbrand_gesamt.json wird nicht überschrieben.")
else:
    with open("waldbrand_gesamt.json", "w", encoding="utf-8") as f:
        json.dump(gesamt_daten, f, ensure_ascii=False, indent=2)
    print("✅ Gesamtdatei gespeichert: waldbrand_gesamt.json")

# ---------------------------------------------------------------------------
# Geokoordinaten ergänzen
# Strategie:
#   1. DWD-Stationsliste (opendata.dwd.de) als primäre Quelle – offizielle Koordinaten
#   2. MOSMIX-Katalog als Fallback, mit Deutschland-Begrenzung
# ---------------------------------------------------------------------------

# Deutschland-Begrenzung für Plausibilitätsprüfung
DE_LAT_MIN, DE_LAT_MAX = 47.0, 56.0
DE_LON_MIN, DE_LON_MAX = 5.0, 16.0

def koordinaten_in_deutschland(lat, lon):
    """Prüft ob Koordinaten plausibel in Deutschland liegen."""
    try:
        return DE_LAT_MIN <= float(lat) <= DE_LAT_MAX and DE_LON_MIN <= float(lon) <= DE_LON_MAX
    except (TypeError, ValueError):
        return False


# --- Schritt 1: DWD-Stationsliste laden ---
print("📍 Lade DWD-Stationsliste von opendata.dwd.de...")
dwd_station_coords = {}  # norm_name -> (lat, lon)
DWD_STATION_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"

try:
    import urllib.request
    with urllib.request.urlopen(DWD_STATION_URL, timeout=30) as resp:
        raw_bytes = resp.read()
    # Datei ist Latin-1 kodiert mit festen Spaltenbreiten
    raw_text = raw_bytes.decode("latin-1")
    lines = raw_text.strip().split("\n")
    # Erste 2 Zeilen sind Header
    for line in lines[2:]:
        if len(line) < 60:
            continue
        try:
            # Feste Spaltenbreiten: ID(0-5), von(6-14), bis(15-23), Höhe(24-38), Breite(39-50), Länge(51-60), Name(61-...)
            lat = float(line[39:50].strip())
            lon = float(line[51:60].strip())
            # Stationsname beginnt ab Spalte 61, Bundesland folgt danach
            rest = line[61:].strip()
            # Name und Bundesland sind durch viele Leerzeichen getrennt
            parts = re.split(r"\s{2,}", rest)
            name = parts[0].strip() if parts else ""
            if name and koordinaten_in_deutschland(lat, lon):
                norm = normalize_name(name)
                dwd_station_coords[norm] = (lat, lon)
        except (ValueError, IndexError):
            continue
    print(f"   ✅ {len(dwd_station_coords)} DWD-Stationen mit Koordinaten geladen")
except Exception as e:
    print(f"   ⚠️ DWD-Stationsliste konnte nicht geladen werden: {e}")


# --- Schritt 2: MOSMIX-Katalog laden (Fallback) ---
print("📍 Lade MOSMIX-Katalog als Fallback...")
try:
    df_mosmix = pd.read_csv("mosmix_stationskatalog1.txt", sep="\t", encoding="utf-8", usecols=["NAME", "LAT", "LON"])
    df_mosmix = df_mosmix.rename(columns={"NAME": "Station", "LAT": "Latitude", "LON": "Longitude"})
    df_mosmix["Latitude"] = pd.to_numeric(df_mosmix["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
    df_mosmix["Longitude"] = pd.to_numeric(df_mosmix["Longitude"].astype(str).str.replace(",", "."), errors="coerce")
    # Nur Einträge mit plausiblen Deutschland-Koordinaten behalten
    df_mosmix = df_mosmix[
        df_mosmix.apply(lambda r: koordinaten_in_deutschland(r["Latitude"], r["Longitude"]), axis=1)
    ].copy()
    df_mosmix["norm_station"] = df_mosmix["Station"].apply(normalize_name)
    station_list = df_mosmix["norm_station"].tolist()
    print(f"   ✅ {len(df_mosmix)} MOSMIX-Stationen in Deutschland geladen")
except Exception as e:
    print(f"   ⚠️ MOSMIX-Katalog konnte nicht geladen werden: {e}")
    df_mosmix = pd.DataFrame()
    station_list = []

# TF-IDF vorbereiten (nur mit deutschen MOSMIX-Einträgen)
tfidf_vectorizer = None
station_matrix = None
if len(station_list) > 0:
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        tfidf_vectorizer = TfidfVectorizer().fit(station_list)
        station_matrix = tfidf_vectorizer.transform(station_list)
    except ImportError:
        pass


# --- Schritt 3: Koordinaten zuordnen ---
print("📍 Ordne Koordinaten zu...")
unmatched = []
gefundene_dwd = 0
gefundene_mosmix = 0

for eintrag in gesamt_daten:
    station_raw = eintrag.get("Stationsname", "")
    norm_name = normalize_name(station_raw)
    tokens = norm_name.split()
    gefunden = False

    # --- Priorität 1: DWD-Stationsliste ---
    # a) Vollständiger normalisierter Name
    if norm_name in dwd_station_coords:
        lat, lon = dwd_station_coords[norm_name]
        eintrag["Latitude"] = lat
        eintrag["Longitude"] = lon
        gefundene_dwd += 1
        gefunden = True
        print(f"🏛️ DWD-Match (exakt): '{station_raw}' → ({lat}, {lon})")
    else:
        # b) Token-Match: Versuche jeden Token als vollständigen DWD-Stationsnamen
        for token in tokens:
            if token in dwd_station_coords:
                lat, lon = dwd_station_coords[token]
                eintrag["Latitude"] = lat
                eintrag["Longitude"] = lon
                gefundene_dwd += 1
                gefunden = True
                print(f"🏛️ DWD-Match (Token): '{station_raw}' → '{token}' → ({lat}, {lon})")
                break

    if not gefunden:
        # c) Token-in-Name: Prüfe ob ein Token des Suchbegriffs in einem DWD-Stationsnamen vorkommt
        for token in tokens:
            kandidaten = [(k, v) for k, v in dwd_station_coords.items() if token in k.split()]
            if len(kandidaten) == 1:
                dwd_name, (lat, lon) = kandidaten[0]
                eintrag["Latitude"] = lat
                eintrag["Longitude"] = lon
                gefundene_dwd += 1
                gefunden = True
                print(f"🏛️ DWD-Match (Token-in-Name): '{station_raw}' → '{dwd_name}' → ({lat}, {lon})")
                break

    if gefunden:
        continue

    # --- Priorität 2: MOSMIX-Katalog (nur Deutschland-Einträge) ---
    if df_mosmix.empty:
        unmatched.append(station_raw)
        continue

    match = pd.DataFrame()

    # Exakter Token-Match (bevorzugt eindeutige)
    for token in tokens:
        temp_match = df_mosmix[df_mosmix["norm_station"] == token]
        if temp_match.shape[0] == 1:
            match = temp_match
            break

    # Subset-Match: alle Tokens in einem Katalogeintrag enthalten
    if match.shape[0] != 1:
        token_set = set(tokens)
        for _, row in df_mosmix.iterrows():
            station_tokens = set(row["norm_station"].split())
            if token_set.issubset(station_tokens):
                match = pd.DataFrame([row])
                break

    if match.shape[0] == 1:
        m = match.iloc[0]
        eintrag["Latitude"] = float(m["Latitude"])
        eintrag["Longitude"] = float(m["Longitude"])
        gefundene_mosmix += 1
        print(f"🔍 MOSMIX-Match: '{station_raw}' → '{m['norm_station']}' → ({m['Latitude']}, {m['Longitude']})")
        continue

    # TF-IDF Matching als letzter Fallback
    if tfidf_vectorizer is not None and station_matrix is not None:
        query_vec = tfidf_vectorizer.transform([norm_name])
        similarities = cosine_similarity(query_vec, station_matrix).flatten()
        best_idx = similarities.argmax()
        best_score = similarities[best_idx]
        if best_score > 0.4:
            match_row = df_mosmix.iloc[best_idx]
            eintrag["Latitude"] = float(match_row["Latitude"])
            eintrag["Longitude"] = float(match_row["Longitude"])
            gefundene_mosmix += 1
            print(f"📐 TF-IDF-Match: '{station_raw}' → '{match_row['norm_station']}' → ({match_row['Latitude']}, {match_row['Longitude']})")
            continue

    unmatched.append(station_raw)
    print(f"❓ Kein Match für '{station_raw}'")

print(f"\n✅ Koordinaten ergänzt: {gefundene_dwd} via DWD, {gefundene_mosmix} via MOSMIX")
print(f"ℹ️ Keine Zuordnung für {len(unmatched)} Stationen: {unmatched}")


# --- Schritt 4: Datei mit Koordinaten speichern ---
if len(gesamt_daten) == 0:
    print("⚠️ Keine Gesamtdaten vorhanden – Koordinaten-Writeback wird übersprungen.")
else:
    with open("waldbrand_gesamt.json", "w", encoding="utf-8") as f:
        for eintrag in gesamt_daten:
            lat = eintrag.get("Latitude")
            lon = eintrag.get("Longitude")
            if not isinstance(lat, (int, float)) or pd.isna(lat):
                eintrag["Latitude"] = 0.0
            if not isinstance(lon, (int, float)) or pd.isna(lon):
                eintrag["Longitude"] = 0.0
            # Plausibilitätsprüfung: ungültige Koordinaten auf 0.0 setzen
            if not koordinaten_in_deutschland(eintrag["Latitude"], eintrag["Longitude"]):
                eintrag["Latitude"] = 0.0
                eintrag["Longitude"] = 0.0
            eintrag["pipeline_run_utc"] = run_ts
        json.dump(gesamt_daten, f, ensure_ascii=False, indent=2)
    print("✅ Gesamtdatei mit Koordinaten gespeichert: waldbrand_gesamt.json")
