from meteostat import Point, Daily
import pandas as pd
from datetime import datetime
import json

cities = {
    "Boston": Point(42.361145, -71.057083)
}

start = datetime(2010, 1, 1)
end = datetime(2010, 1, 2)

for city_name, location in cities.items():
    # Récupération des données Meteostat
    data = Daily(location, start, end).fetch()

    # Convertir l'index en datetime aware UTC
    data.index = pd.to_datetime(data.index, unit='ms', utc=True)

    # Transformer l'index en colonne 'date' et convertir en chaîne
    daily_list = data.copy()
    daily_list['date'] = daily_list.index.astype(str)  # <-- correction ici
    daily_list = daily_list.reset_index(drop=True)

    # Transformer en liste de dictionnaires
    daily_list = daily_list.to_dict(orient='records')

    # Sauvegarde en JSON
    filename = city_name.lower() + "_weather.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(daily_list, f, ensure_ascii=False, indent=2)

    print(f"Saved data for {city_name} in {filename}")