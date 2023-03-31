import time

from demo.D00_init_server_setup import TOR_ROUTER, MON_ROUTER, WIN_ROUTER, \
    ON_REPLSET, ON_REPL_MON, ON_REPL_WIN
from demo.simple_data import insert_sample_data, clear_data, \
    query_from_windsor, query_from_cornwall, print_demo_title

"""
Demo notes:
Data is simplified for the purposes of demonstration
- we load sample data from two locations and two times
    - OTTAWA/TORONTO
    - TODAY/YESTERDAY
- the data is loaded using the Python REST API
- the queries find the most recent data from the nearest weather station
    - based on timestamp/geolocation fields
    - we add location/dateTime for readability
        - these fields have different values in the actual data
- we use the Python REST API to search from the latitude/longitude
    - CORNWALL/WINDSOR
- expect that
    - querying from WINDSOR returns TORONTO data
    - querying from CORNWALL returns OTTAWA data
"""


# 1. Insert/query data with all servers running
"""
Demo notes
- this is to verify the querying works as expected (see notes at top)
"""

print_demo_title(1, "get the weather")

insert_sample_data(detailed=True)

"""
fields of interest:
- dateTime (placeholder) : TODAY/YESTERDAY
    - no YESTERDAY is shown
    - this query is based on timestamp
- location (placeholder): OTTAWA/TORONTO
    - CORNWALL is closer to OTTAWA
    - WINDSOR is close to TORONTO
    - this query is based on geolocation
    - the distance between the query location and the weather station location:
        - the calculated field distanceToWeatherStation
        - calculated when the query finds the nearest weather station
        - distance is in m
        - geometry is NOT euclidean, but a 2d-sphere
            - good approximation to earth's curvature
"""
query_from_windsor()
query_from_cornwall()

clear_data()

# 2. Bringing down routers
"""
Demo notes:
- we bring two routers offline and verify the REST API still works
- then bring down the router that was online, bring up one of the offline
routers, and verify the REST API still works

The REST API has connection urls to all of the routers
- goes through each one and performs a health check
    - checks server
    - pings mongo instance running on the server
"""

print_demo_title(2, "bringing down routers")

print("Bringing Toronto and Montreal routers offline")
TOR_ROUTER.shutdown()
MON_ROUTER.shutdown()
while TOR_ROUTER.healthy() or MON_ROUTER.healthy():
    time.sleep(2)
print("  Toronto and Montreal routers offline")
input("Press enter to continue")

insert_sample_data(detailed=True)

query_from_windsor()

print("Bringing Winnipeg router offline and Montreal router online")
MON_ROUTER.startup()
while not MON_ROUTER.healthy():
    time.sleep(2)
WIN_ROUTER.shutdown()
while WIN_ROUTER.healthy():
    time.sleep(2)
print("  Winnipeg router offline and Montreal router online")
input("Press enter to continue")

query_from_cornwall()

print("Bringing routers online")
TOR_ROUTER.startup()
WIN_ROUTER.startup()
while not TOR_ROUTER.healthy() or not WIN_ROUTER.healthy():
    time.sleep(2)
print("  Routers online")

clear_data()

time.sleep(10)

# 3. Bringing down data servers

"""
Demo Notes:
- bring down the primary data server
    - confirm writes still work
- bring down another data server
    - confirm reads still work

Note: Bringing down config servers is the same (replica set)
"""
print_demo_title(3, "bringing down data servers")


print(f"Bringing down {ON_REPLSET.replica_set_name} primary data server "
      f"{ON_REPLSET.pref_primary.container_name}")
ON_REPLSET.pref_primary.shutdown()
while ON_REPLSET.pref_primary.healthy():
    time.sleep(1)
print("  Data server is down")
input("Press enter to continue")

insert_sample_data()

# Note: We don't need to do this with >3 shard servers in a replica set
print("Cycling primary data server for replication")
print("  Bringing up primary data server")
ON_REPLSET.pref_primary.startup()
while not ON_REPLSET.pref_primary.healthy():
    time.sleep(2)

time.sleep(10)

print("  Bringing down primary data server")
ON_REPLSET.pref_primary.shutdown()
while ON_REPLSET.pref_primary.healthy():
    time.sleep(1)

print(f"Bringing down Winnipeg data server")
ON_REPL_WIN.shutdown()
while ON_REPL_WIN.healthy():
    time.sleep(1)
print("  Brought down Winnipeg data server")

query_from_cornwall()
query_from_windsor()

"""
Demo notes

- we have 2/3 instances down
    - the replica set will not have enough members to elect a PRIMARY
    - the only instance up right now is a SECONDARY
- we need to have a majority of instances up for writes to work
"""
print("Bringing up Winnipeg data server for writes")
ON_REPL_WIN.startup()
while not ON_REPL_WIN.healthy():
    time.sleep(1)
print("  Winnipeg data server is up")
input("Press enter to continue")

time.sleep(10)

clear_data()