from datetime import datetime

from bson import ObjectId
from starlette.testclient import TestClient

from api.db import DB_NAME, COLLECTION_NAME
from api.main import app, get_router
from demo.D00_init_server_setup import TOR_ROUTER

API_CLIENT: TestClient = TestClient(app)

OTTAWA_LOC: dict = {
    "lon": -75, "lat": 45
}
OTTAWA_GEOJSON: dict = {
    "type": "Point",
    "coordinates": [OTTAWA_LOC["lon"], OTTAWA_LOC["lat"]]
}

TORONTO_LOC: dict = {
    "lon": -79, "lat": 45
}
TORONTO_GEOJSON: dict = {
    "type": "Point",
    "coordinates": [TORONTO_LOC["lon"], TORONTO_LOC["lat"]]
}

TIME_FMT: str = "%Y-%m-%dT%H:%M:%S.%fZ"
TODAY_TIMESTAMP = datetime(2023, 3, 26).strftime(TIME_FMT)
YESTERDAY_TIMESTAMP = datetime(2023, 3, 25).strftime(TIME_FMT)

WINDSOR_LOC: dict = {
    "lon": -83, "lat": 42
}

CORNWALL_LOC: dict = {
    "lon": -74, "lat": 45
}

sample_data = [
    {
        "location": "OTTAWA",
        "dateTime": "TODAY",
        "geolocation": OTTAWA_GEOJSON,
        "timestamp": TODAY_TIMESTAMP
    },
    {
        "location": "OTTAWA",
        "dateTime": "YESTERDAY",
        "geolocation": OTTAWA_GEOJSON,
        "timestamp": YESTERDAY_TIMESTAMP
    },
    {
        "location": "TORONTO",
        "dateTime": "TODAY",
        "geolocation": TORONTO_GEOJSON,
        "timestamp": TODAY_TIMESTAMP
    },
    {
        "location": "TORONTO",
        "dateTime": "YESTERDAY",
        "geolocation": TORONTO_GEOJSON,
        "timestamp": YESTERDAY_TIMESTAMP
    },
]


def insert_sample_data(detailed=False):
    print("Using REST POST endpoint to load sample data into db...")
    for data in sample_data:
        if detailed:
            print(f"  Inserting: {data}")
        json = API_CLIENT.post("/weather/post", json=data).json()
        data["_id"] = ObjectId(json["_id"])
        if detailed:
            print(f"  Response body: {json}")
    if detailed:
        input("  Data loaded: Press enter to continue...")
    else:
        print("  Data loaded")


def query_from_cornwall():
    print(f"Querying weather near Cornwall (coords: {CORNWALL_LOC})...")
    resp = API_CLIENT.post("/weather/get", json=CORNWALL_LOC).json()
    print(f"  Found: {resp}")
    input("Press enter to continue...")


def query_from_windsor():
    print(f"Querying weather near Windsor (coords: {WINDSOR_LOC})...")
    resp = API_CLIENT.post("/weather/get", json=WINDSOR_LOC).json()
    print(f"  Found: {resp}")
    input("Press enter to continue...")


def clear_data():
    print("Deleting all sample data...")
    conn = get_router().connect()
    collection = conn.get_database(DB_NAME) \
        .get_collection(COLLECTION_NAME)

    for data in sample_data:
        collection.find_one_and_delete({"_id": data["_id"]})
        del data["_id"]

    conn.close()
    print("  Data deleted")


def print_demo_title(idx: int, message: str):
    print(f"\n-----DEMO {idx}: {message.upper()}-----")