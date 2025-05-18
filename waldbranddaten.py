import pandas as pd

url = "https://www.dwd.de/DE/leistungen/waldbrandgef/waldbrandgef.html"

dfs = pd.read_html(url)

waldbrand_df = None
for df in dfs:
    if "Bundesland" in df.columns[0]:
        waldbrand_df = df.copy()
        break

if waldbrand_df is not None:
    waldbrand_df.columns = ["Bundesland", "Waldbrandstufe"]
    waldbrand_df["Bundesland"] = waldbrand_df["Bundesland"].str.strip()
    waldbrand_df["Waldbrandstufe"] = (
        waldbrand_df["Waldbrandstufe"]
        .astype(str)
        .str.extract(r"(\d)")
        .astype(float)
    )

    waldbrand_df.to_json("waldbrand.json", orient="records", force_ascii=False, indent=2)
    print("waldbrand.json erfolgreich erstellt.")
else:
    print("❌ Keine passende Tabelle gefunden.")