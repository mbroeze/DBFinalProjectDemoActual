from pymongo import MongoClient, ReadPreference
from pymongo.collection import Collection
from pymongo.results import InsertOneResult

from base.db_physical import MongoRouter

DB_NAME: str = "env-canada"
COLLECTION_NAME: str = "weather"


def read_client(router: MongoRouter) -> Collection:
    url: str = router.mongo_url()
    return MongoClient(url)\
        .get_database(DB_NAME)\
        .get_collection(
            COLLECTION_NAME,
            read_preference=ReadPreference.PRIMARY_PREFERRED
    )


def weather_doc_to_dict(weather_doc) -> dict:
    data = {"_id": str(weather_doc["_id"])}
    fields: list["str"] = [
        "license", "timestamp", "geolocation",
        "distanceToWeatherStation", "location", "dateTime", "warnings",
        "currentConditions", "forecastGroup", "hourlyForecastGroup",
        "yesterdayConditions", "riseSet", "almanac"
    ]
    for field in fields:
        try:
            data[field] = weather_doc[field]
        except KeyError:
            pass
    return data


def check_weather(router: MongoRouter, lon: float, lat: float) -> dict:
    url: str = router.mongo_url()
    conn = MongoClient(url)
    coll = conn.get_database(DB_NAME) \
        .get_collection(
        COLLECTION_NAME,
        read_preference=ReadPreference.PRIMARY_PREFERRED
    )
    doc = coll.aggregate([
        {
            "$geoNear": {
                "near": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "key": "geolocation",
                "spherical": True,
                "distanceField": "distanceToWeatherStation"
            }
        },
        {
            "$sort": {"distanceToWeatherStation": 1, "timestamp": -1}
        }
    ]).next()
    conn.close()
    return weather_doc_to_dict(doc)


def insert_weather(router: MongoRouter, data: dict) -> InsertOneResult:
    url: str = router.mongo_url()
    conn = MongoClient(url)
    coll = conn.get_database(DB_NAME)\
        .get_collection(
        COLLECTION_NAME
    )
    output = coll.insert_one(data)
    conn.close()
    return output
