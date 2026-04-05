import pandas as pd
import json
import re
import os
from datetime import datetime, timezone

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
        if "HTTP Error 404" in msg or "404" in msg:
            fetch_404 += 1
            if os.path.exists(outfile):
                print(f"🧊 Off-Season/404 für {name} ({kuerzel}) – verwende bestehende Datei: {outfile}")
            else:
                print(f"🧊 Off-Season/404 für {name} ({kuerzel}) – keine bestehende Datei vorhanden")
        else:
            fetch_other_err += 1
            print(f"❌ Fehler bei {name} ({kuerzel}): {e}")


# ---------------------------------------------------------------------------
# Gesamtdaten zusammenführen und als waldbrand_daten.json schreiben
# Format: Dictionary mit Stationsname als Key (für schnellen Lookup in der App)
# ---------------------------------------------------------------------------

daten_dict = {}
stationsnamen_col = "Stationsname"

for kuerzel in bundeslaender.keys():
    dateiname = f"waldbrand_{kuerzel}.json"
    if not os.path.exists(dateiname):
        continue
    with open(dateiname, encoding="utf-8") as f:
        eintraege = json.load(f)
    for eintrag in eintraege:
        name = eintrag.pop(stationsnamen_col, None)
        if not name:
            continue
        # Nur die Datums-Werte übernehmen (alles außer bekannte Meta-Felder)
        werte = {}
        for key, val in eintrag.items():
            if key in ("Bundesland", "Latitude", "Longitude", "pipeline_run_utc", "Vorschlaege"):
                continue
            werte[key] = str(val)
        daten_dict[name] = werte

meta = {
    "run_timestamp_utc": run_ts,
    "source": "DWD wbx_tab_alle_{BL}.html",
    "fetch_ok": fetch_ok,
    "fetch_404": fetch_404,
    "fetch_other_err": fetch_other_err,
    "offseason_expected": datetime.now().month in OFFSEASON_MONTHS,
    "data_status": "offseason_or_unavailable" if (fetch_ok == 0 and fetch_404 > 0) else "fresh_or_partial"
}

waldbrand_response = {
    "meta": meta,
    "daten": daten_dict
}

if len(daten_dict) == 0:
    print("⚠️ Keine Daten vorhanden – waldbrand_daten.json wird nicht überschrieben.")
else:
    with open("waldbrand_daten.json", "w", encoding="utf-8") as f:
        json.dump(waldbrand_response, f, ensure_ascii=False, indent=2)
    print(f"✅ waldbrand_daten.json gespeichert: {len(daten_dict)} Stationen")

# Meta separat (für Abwärtskompatibilität)
with open("waldbrand_meta.json", "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)
print(f"🧾 Meta gespeichert: waldbrand_meta.json ({meta['data_status']})")


# ---------------------------------------------------------------------------
# Abwärtskompatibilität: waldbrand_gesamt.json weiterhin erzeugen
# Kann nach App-Migration entfernt werden.
# ---------------------------------------------------------------------------

if os.path.exists("stationen.json"):
    with open("stationen.json", encoding="utf-8") as f:
        registry = json.load(f)
    station_coords = {s["name"]: s for s in registry.get("stationen", [])}
else:
    station_coords = {}

gesamt_daten = []
for name, werte in daten_dict.items():
    eintrag = {"Stationsname": name}
    eintrag.update(werte)
    if name in station_coords:
        s = station_coords[name]
        eintrag["Latitude"] = s["lat"]
        eintrag["Longitude"] = s["lon"]
        eintrag["Bundesland"] = s["bundesland"]
    else:
        eintrag["Latitude"] = 0.0
        eintrag["Longitude"] = 0.0
        eintrag["Bundesland"] = ""
    eintrag["pipeline_run_utc"] = run_ts
    gesamt_daten.append(eintrag)

if gesamt_daten:
    with open("waldbrand_gesamt.json", "w", encoding="utf-8") as f:
        json.dump(gesamt_daten, f, ensure_ascii=False, indent=2)
    print(f"✅ waldbrand_gesamt.json gespeichert (Kompatibilität): {len(gesamt_daten)} Stationen")
