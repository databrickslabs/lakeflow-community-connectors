from sources.open_meteo.open_meteo import LakeflowConnect

if __name__ == "__main__":
    # Initialize (no auth required!)
    connector = LakeflowConnect({})

    # Get Berlin weather forecast
    records, offset = connector.read_table("forecast", {}, {
        "latitude": "52.52",
        "longitude": "13.41",
        "hourly": "temperature_2m,precipitation,weather_code",
        "forecast_days": "7"
    })

    for record in records:
        print(f"{record['time']}: {record['temperature_2m']}°C")