from meteostat import Point, Hourly
import pandas as pd
from datetime import datetime

cities = {
    "New York": Point(40.7128, -74.0060),
    "Chicago": Point(41.8781, -87.6298),
    "Boston": Point(42.361145, -71.057083),
    "Tokyo": Point(35.652832, 139.839478),
    "Londres": Point(51.509865, -0.118092),
    "Sydney": Point(-33.865143, 151.209900),
    "Berlin": Point(52.520008, 13.404954)
}

# A changer selon les besoins
start = datetime(2010, 1, 1)
end = datetime(2024, 12, 31)

for city_name, location in cities.items():
    data = Hourly(location, start, end)
    data = data.fetch()

    filename = city_name.lower() + "_weather.json"
    data.to_json(filename)
    print(f"Saved data for {city_name} in {filename}")