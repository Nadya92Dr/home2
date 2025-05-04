import os
import requests
import csv
from datetime import datetime
from dotenv import load_dotenv


def fetch_weather(api_key: str, city="Moscow") -> dict:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def append_weather_data(api_key: str, city="Moscow"):
    data = fetch_weather(api_key, city)

    row = {
        "datetime": data["dt"],
        "city": city,
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "temp": data["main"]["temp"],
        "feels_like": data["main"]["feels_like"],
        "pressure": data["main"]["pressure"],
        "wind_speed": data["wind"]["speed"],
    }

    row["datetime"] = datetime.utcfromtimestamp(row["datetime"]).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    file_exists = os.path.isfile("weather.csv")
    with open("weather.csv", "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


if __name__ == "__main__":
    load_dotenv(".env")
    api_key = os.getenv("API_KEY")
    append_weather_data(api_key=api_key, city="Moscow")
    print("Данные успешно добавлены в weather.csv")
