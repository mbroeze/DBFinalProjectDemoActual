import time

from pymongo.errors import OperationFailure

from base.db_logical import ConfigServerReplicaSet, ShardServerReplicaSet
from base.db_physical import DataCentre, MongoRouter, MongoConfigServer

"""
Sets up Mongo as follows:
- 3x router
- 3x config server
- 1x shard replica set with 3x nodes

Notes
- sharding is NOT enabled at this time
- only one replica set (we add this later to demo scaling)
- everything with a router takes a while
"""

print("Creating sharded cluster (config servers, routers, shards)")
print("------------")

# 1. Data Centre Representation

print("Initiating data centres...")

print("  Initiating Toronto data centre")
TOR_DC: DataCentre = DataCentre(
    location="toronto", start_port=27020
)
print("  Initiating Winnipeg data centre")
WIN_DC: DataCentre = DataCentre(
    location="winnipeg", start_port=27030
)
print("  Initiating Montreal data centre")
MON_DC: DataCentre = DataCentre(
    location="montreal", start_port=27040
)

# 2. Config server replica sets

print("Initiating config server replica sets...")

CFG_SVR_REPLSET: ConfigServerReplicaSet = ConfigServerReplicaSet(
    replica_set_name="cfg"
)

print("  Creating Toronto config server")
TOR_CFG: MongoConfigServer = CFG_SVR_REPLSET.add_server(
    TOR_DC, pref_primary=True
)
print("  Creating Winnipeg config server")
WIN_CFG: MongoConfigServer = CFG_SVR_REPLSET.add_server(WIN_DC)
print("  Creating Montreal config server")
MON_CFG: MongoConfigServer = CFG_SVR_REPLSET.add_server(MON_DC)

print("Waiting for config servers to come online...")
CFG_SVR_REPLSET.wait_until_healthy()
print("  Config servers online")

print("Initiating config server replica set...")
try:
    output = CFG_SVR_REPLSET.initiate_replica_set()
    print(f"  Output: {output}")
except OperationFailure as err:
    print("  Config server replica set already initialized")

# 3. Routers

print("Initiating routers...")
print("  Initiating Toronto router")
TOR_ROUTER = TOR_DC.add_router(config_servers=CFG_SVR_REPLSET.config_servers)
print("  Initiating Winnipeg router")
WIN_ROUTER = WIN_DC.add_router(config_servers=CFG_SVR_REPLSET.config_servers)
print("  Initiating Montreal router")
MON_ROUTER = MON_DC.add_router(config_servers=CFG_SVR_REPLSET.config_servers)

ROUTERS: list[MongoRouter] = [
    *TOR_DC.routers, *WIN_DC.routers, *MON_DC.routers
]

print("Waiting for routers to come online...")
for router in ROUTERS:
    while not router.healthy():
        time.sleep(1)
print("  Routers have come online")

# 4. Shard Server Replica Set

print("Initiating shard server replica set...")

ON_REPLSET: ShardServerReplicaSet = ShardServerReplicaSet(
    replica_set_name="ONTARIO"
)
print("  Initiating Toronto shard server")
ON_REPL_TOR = ON_REPLSET.add_server(dc=TOR_DC, pref_primary=True)
print("  Initiating Winnipeg shard server")
ON_REPL_WIN = ON_REPLSET.add_server(dc=WIN_DC)
print("  Initiating Montreal shard server")
ON_REPL_MON = ON_REPLSET.add_server(dc=MON_DC)

print("Waiting for shard servers to come online...")
ON_REPLSET.wait_until_healthy()
print("  Shard servers have come online")

print("Initiating Ontario shard replica set...")
try:
    output = ON_REPLSET.initiate_replica_set()
    print(f"  Output: {output}")

    print("Connecting routers to Ontario shard replica set")
    if not TOR_ROUTER.has_shard(ON_REPLSET.pref_primary):
        output = TOR_ROUTER.add_shard(ON_REPLSET.pref_primary)
        print(f" Output {output}")
    else:
        print("  Routers already connected to Ontario shard replica set")
except OperationFailure as err:
    print("  Ontario shard replica set already initialized")




