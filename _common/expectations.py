CITY_REQUIRED_COLS = ["city_id", "city_name"]

TRIPS_REQUIRED_COLS = [
    "trip_id",
    "city_id",
    "date",
    "distance_travelled_km",
    "passenger_type",
    "fare_amount",
    "passenger_rating",
    "driver_rating",
]

HASH_COLS = {
    "city": ["city_id", "city_name"],
    "trips": ["trip_id", "city_id", "date", "distance_travelled_km", "passenger_type"],
}

TYPE_DRIFT_FIELDS = {
    "city": {"city_id": "string", "city_name": "string"},
    "trips": {
        "trip_id": "string",
        "city_id": "string",
        "date": "date",
        "distance_travelled_km": "double",
        "passenger_type": "string",
        "fare_amount": "double",
        "passenger_rating": "int",
        "driver_rating": "int",
    },
}
