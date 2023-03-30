from pymongo.errors import OperationFailure

from base.db_logical import ShardServerReplicaSet

from demo.D00_init_server_setup import TOR_DC, WIN_DC, MON_DC, TOR_ROUTER

# 1. Create two new replica sets, Manitoba and Quebec
print("Creating Manitoba shard server replica set...")

MB_REPLSET: ShardServerReplicaSet = ShardServerReplicaSet(
    replica_set_name="MANITOBA"
)

print("  Initiating Toronto shard server")
MB_REPL_TOR = MB_REPLSET.add_server(dc=TOR_DC)
print("  Initiating Winnipeg shard server")
MB_REPL_WIN = MB_REPLSET.add_server(dc=WIN_DC, pref_primary=True)
print("  Initiating Montreal shard server")
MB_REPL_MON = MB_REPLSET.add_server(dc=MON_DC)

print("Waiting for Manitoba shard servers to come online...")
MB_REPLSET.wait_until_healthy()
print("  Shard servers have come online")

print("Creating Quebec shard server replica set...")

QC_REPLSET: ShardServerReplicaSet = ShardServerReplicaSet(
    replica_set_name="QUEBEC"
)

print("  Initiating Toronto shard server")
QC_REPL_TOR = QC_REPLSET.add_server(dc=TOR_DC)
print("  Initiating Winnipeg shard server")
QC_REPL_WIN = QC_REPLSET.add_server(dc=WIN_DC)
print("  Initiating Montreal shard server")
QC_REPL_MON = QC_REPLSET.add_server(dc=MON_DC, pref_primary=True)

print("Waiting for Quebec shard servers to come online...")
QC_REPLSET.wait_until_healthy()
print("  Shard servers have come online")

# 2. Initiate the replica sets, and connect them to the routers

print("Initiating Manitoba shard replica set...")
try:
    output = MB_REPLSET.initiate_replica_set()
    print(f"  Output: {output}")

    print("Connecting routers to Manitoba shard replica set")
    if not TOR_ROUTER.has_shard(MB_REPLSET.pref_primary):
        output = TOR_ROUTER.add_shard(MB_REPLSET.pref_primary)
        print(f" Output {output}")
    else:
        print("  Routers already connected to Manitoba shard replica set")
except OperationFailure as err:
    print("  Manitoba shard replica set already initialized")

print("Initiating Quebec shard replica set...")
try:
    output = QC_REPLSET.initiate_replica_set()
    print(f"  Output: {output}")

    print("Connecting routers to Quebec shard replica set")
    if not TOR_ROUTER.has_shard(QC_REPLSET.pref_primary):
        output = TOR_ROUTER.add_shard(QC_REPLSET.pref_primary)
        print(f" Output {output}")
    else:
        print("  Routers already connected to Quebec shard replica set")
except OperationFailure as err:
    print("  Quebec shard replica set already initialized")
