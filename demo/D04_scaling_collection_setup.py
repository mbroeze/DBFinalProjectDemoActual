from bson import MinKey, MaxKey

from base.db_logical import ShardServerReplicaSet
from demo.D00_init_server_setup import TOR_ROUTER, ON_REPLSET
from demo.D01_init_collection_setup import DB_NAME, COLLECTION_NAME
from demo.D03_scaling_server_setup import MB_REPLSET, QC_REPLSET

# 1. Shard Collection

print("Creating shard key index (longitude of weather station)")
conn = TOR_ROUTER.connect()
collection = conn\
    .get_database(DB_NAME)\
    .get_collection(COLLECTION_NAME)

SHARD_KEY = "stationLongitude"
output = collection.create_index([(SHARD_KEY, 1)])
print(f"  Output: {output}")
print(f"  Shard key index created on {SHARD_KEY}")

print("Sharding collection")
cmd: dict = {
            'shardCollection': f"{DB_NAME}.{COLLECTION_NAME}",
            'key': {SHARD_KEY: 1}
        }
output = conn.admin.command(cmd)
print(f"  Output: {output}")

# 2. Create zones

repl_sets: list[ShardServerReplicaSet] = [
    ON_REPLSET,
    MB_REPLSET,
    QC_REPLSET
]

print("Creating zones")
for repl_set in repl_sets:
    print(f"  Creating {repl_set.replica_set_name} zone")
    output = conn.admin.command(
        "addShardToZone",
        repl_set.replica_set_name,
        zone=repl_set.replica_set_name
    )
    print(f"  Output: {output}")
print("  Zones created")

print("Partitioning zones")
TBAY_LON = -89.3
OTT_LON = -75.7

print(f"Partitioning {MB_REPLSET.replica_set_name} zone west of Thunder Bay")
output = conn.admin.command(
    "updateZoneKeyRange",
    f"{DB_NAME}.{COLLECTION_NAME}",
    min={SHARD_KEY: MinKey()},  # incl.
    max={SHARD_KEY: TBAY_LON},  # excl
    zone=MB_REPLSET.replica_set_name
)
print(f"  {output}")

print(f"Partitioning {ON_REPLSET.replica_set_name} zone between Thunder Bay "
      f"and Ottawa")
output = conn.admin.command(
    "updateZoneKeyRange",
    f"{DB_NAME}.{COLLECTION_NAME}",
    min={SHARD_KEY: TBAY_LON},  # incl.
    max={SHARD_KEY: OTT_LON},  # excl
    zone=ON_REPLSET.replica_set_name
)
print(f"  {output}")

print(f"Partitioning {QC_REPLSET.replica_set_name} zone east of Ottawa")
output = conn.admin.command(
    "updateZoneKeyRange",
    f"{DB_NAME}.{COLLECTION_NAME}",
    min={SHARD_KEY: OTT_LON},  # incl.
    max={SHARD_KEY: MaxKey()},  # excl
    zone=QC_REPLSET.replica_set_name
)
print(f"  {output}")

conn.close()

"""
You are now ready to scrape data
- start the api server from the root directory of this project
    - uvicorn api.main:app
- start the weather scraper, and let it run for a while
- data will be partitioned across the 3 replica sets
"""