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

 # Geokoordinaten ergänzen auf Basis von mosmix_stationskatalog1.txt
print("📍 Versuche Geokoordinaten über mosmix_stationskatalog1.txt zu ergänzen...")
try:
    df_mosmix = pd.read_csv("mosmix_stationskatalog1.txt", sep="\t", encoding="utf-8", usecols=["NAME", "LAT", "LON"])
    df_mosmix = df_mosmix.rename(columns={"NAME": "Station", "LAT": "Latitude", "LON": "Longitude"})
    df_mosmix["Latitude"] = pd.to_numeric(df_mosmix["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
    df_mosmix["Longitude"] = pd.to_numeric(df_mosmix["Longitude"].astype(str).str.replace(",", "."), errors="coerce")

    df_mosmix["norm_station"] = df_mosmix["Station"].apply(normalize_name)
    station_list = df_mosmix["norm_station"].tolist()

    # Neue Matching-Logik (siehe Aufgabenstellung)
    unmatched = []
    gefundene = 0

    # Vorbereitung für TF-IDF
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        tfidf_vectorizer = TfidfVectorizer().fit(station_list)
        station_matrix = tfidf_vectorizer.transform(station_list)
    except ImportError:
        tfidf_vectorizer = None
        station_matrix = None

    for eintrag in gesamt_daten:
        station_raw = eintrag.get("Stationsname", "")
        norm_name = normalize_name(station_raw)
        tokens = norm_name.split()
        match = pd.DataFrame()

        # Versuche exakten Match mit jedem Token
        for token in tokens:
            temp_match = df_mosmix[df_mosmix["norm_station"] == token]
            if temp_match.shape[0] == 1:
                match = temp_match
                break

        # Wenn kein direkter Match, prüfe, ob alle Tokens in einem norm_station-Eintrag enthalten sind
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
            gefundene += 1
            print(f"🔍 Match: '{station_raw}' → '{m['norm_station']}' → Koordinaten: ({m['Latitude']}, {m['Longitude']})")
        else:
            # Direkt TF-IDF Matching für alle übrigen Fälle
            if tfidf_vectorizer is not None and station_matrix is not None:
                query_vec = tfidf_vectorizer.transform([norm_name])
                similarities = cosine_similarity(query_vec, station_matrix).flatten()
                best_idx = similarities.argmax()
                best_score = similarities[best_idx]
                if best_score > 0.4:
                    best_match_name = station_list[best_idx]
                    match_row = df_mosmix[df_mosmix["norm_station"] == best_match_name].iloc[0]
                    eintrag["Latitude"] = float(match_row["Latitude"])
                    eintrag["Longitude"] = float(match_row["Longitude"])
                    gefundene += 1
                    print(f"📐 TF-IDF-Match: '{station_raw}' → '{best_match_name}' → Koordinaten: ({match_row['Latitude']}, {match_row['Longitude']})")
                else:
                    # Füge Vorschlagsliste hinzu
                    top_indices = similarities.argsort()[-3:][::-1]
                    vorschlaege = []
                    for idx in top_indices:
                        kandidat = station_list[idx]
                        score = similarities[idx]
                        vorschlaege.append({"name": kandidat, "score": round(float(score), 3)})
                    eintrag["Vorschlaege"] = vorschlaege
                    unmatched.append(station_raw)
                    print(f"❓ Kein sicherer Match für '{station_raw}'. Vorschläge: {vorschlaege}")
            else:
                unmatched.append(station_raw)

    print(f"✅ Koordinaten ergänzt für {gefundene} Stationen.")
    print(f"ℹ️ Keine Übereinstimmung für {len(unmatched)} Stationen, z. B.: {unmatched[:5]}")
except Exception as e:
    print(f"❌ Fehler beim Einlesen der MOSMIX-Daten: {e}")

# Datei mit Koordinaten speichern
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
            # Für die App: Zeitstempel des letzten Pipeline-Laufs pro Eintrag
            eintrag["pipeline_run_utc"] = run_ts
        json.dump(gesamt_daten, f, ensure_ascii=False, indent=2)
    print("✅ Gesamtdatei mit Koordinaten gespeichert: waldbrand_gesamt.json")
