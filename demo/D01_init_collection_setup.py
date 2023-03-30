from demo.D00_init_server_setup import TOR_ROUTER

DB_NAME: str = "env-canada"
COLLECTION_NAME: str = "weather"

LOCATION_FIELD: str = "geolocation"
TIMESTAMP_FIELD: str = "timestamp"

conn = TOR_ROUTER.connect()
collection = conn\
    .get_database(DB_NAME)\
    .get_collection(COLLECTION_NAME)

"""
Demo notes:
We create two indices on geolocation and timestamp.

These improve the performance of querying the most recent data from the nearest 
weather station to the user's location.
"""

print("Creating geolocation index")
output = collection.create_index([(LOCATION_FIELD, "2dsphere")])
print(f"  Output: {output}")

print("Creating timestamp index")
output = collection.create_index([(TIMESTAMP_FIELD, -1)])
print(f"  Output: {output}")

conn.close()
