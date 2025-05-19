import pandas as pd
import json
import difflib

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

for kuerzel, name in bundeslaender.items():
    url = base_url.format(kuerzel=kuerzel)
    try:
        dfs = pd.read_html(url)
        print(f"✅ {name} ({kuerzel}): {len(dfs)} Tabellen gefunden")
        if len(dfs) >= 2:
            df = dfs[1]
            # Filtere die Legenden-Zeile heraus (die eine Wiederholung der Spaltennamen ist)
            df = df[df[df.columns[0]] != df.columns[0]]
            df_json = df.to_dict(orient="records")
            with open(f"waldbrand_{kuerzel}.json", "w", encoding="utf-8") as f:
                json.dump(df_json, f, ensure_ascii=False, indent=2)
            print(f"💾 Datei gespeichert: waldbrand_{kuerzel}.json")
        else:
            print(f"⚠️ {name}: Zu wenige Tabellen")
    except Exception as e:
        print(f"❌ Fehler bei {name} ({kuerzel}): {e}")


# Alle einzelnen JSON-Dateien zusammenführen
import os

gesamt_daten = []

for kuerzel in bundeslaender.keys():
    dateiname = f"waldbrand_{kuerzel}.json"
    if os.path.exists(dateiname):
        with open(dateiname, encoding="utf-8") as f:
            eintraege = json.load(f)
            for eintrag in eintraege:
                eintrag["Bundesland"] = bundeslaender[kuerzel]
            gesamt_daten.extend(eintraege)

# Gesamtdatei schreiben
with open("waldbrand_gesamt.json", "w", encoding="utf-8") as f:
    json.dump(gesamt_daten, f, ensure_ascii=False, indent=2)

print("✅ Gesamtdatei gespeichert: waldbrand_gesamt.json")

# Geokoordinaten ergänzen auf Basis von mosmix_stationskatalog.txt
print("📍 Versuche Geokoordinaten über mosmix_stationskatalog.txt zu ergänzen...")
try:
    with open("mosmix_stationskatalog.txt", encoding="utf-8") as f:
        lines = f.readlines()
        entries = []
        for line in lines:
            parts = line.strip().split()
            if len(parts) >= 5 and parts[0].isdigit():
                station_id = parts[0]
                name = " ".join(parts[2:-3])
                lat = parts[-3]
                lon = parts[-2]
                entries.append({"Station": name, "Latitude": lat, "Longitude": lon})

        station_df = pd.DataFrame(entries)
        station_df["Station"] = station_df["Station"].astype(str).str.strip()
        station_list = station_df["Station"].tolist()

        gefundene = 0
        for eintrag in gesamt_daten:
            station = eintrag.get("Stationsname", "").strip()
            close_matches = difflib.get_close_matches(station, station_list, n=1, cutoff=0.85)
            if close_matches:
                match = station_df[station_df["Station"] == close_matches[0]]
                if not match.empty:
                    eintrag["Latitude"] = match.iloc[0]["Latitude"]
                    eintrag["Longitude"] = match.iloc[0]["Longitude"]
                    gefundene += 1
        print(f"✅ Koordinaten ergänzt für {gefundene} Stationen.")
except Exception as e:
    print(f"❌ Fehler beim Einlesen der MOSMIX-Daten: {e}")

# Datei mit Koordinaten speichern
with open("waldbrand_gesamt.json", "w", encoding="utf-8") as f:
    json.dump(gesamt_daten, f, ensure_ascii=False, indent=2)
print("✅ Gesamtdatei mit Koordinaten gespeichert: waldbrand_gesamt.json")
