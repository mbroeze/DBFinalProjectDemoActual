import json
from random import randint

from demo.D00_init_server_setup import TOR_ROUTER, ON_REPL_TOR
from demo.D01_init_collection_setup import DB_NAME, COLLECTION_NAME
from demo.D03_scaling_server_setup import MB_REPL_WIN, QC_REPL_MON
from demo.simple_data import print_demo_title

"""
In the previous demos, we used the REST API to query the database.

For this demo, we will be establishing direct connections with various 
components of the sharded cluster in order to verify that the data is being 
sent to the correct places.
"""

AB_QUERY = {"location.province.code": "ab"}
ON_QUERY = {"location.province.code": "on"}
NB_QUERY = {"location.province.code": "nb"}


def print_doc_info(report):
    data = {
        "prov": report["location"]["province"]["text"],
        "region": report["location"]["region"],
        "temp": report["currentConditions"]["temperature"]["text"] + " C",
        "windchill": report["currentConditions"]["windChill"]["text"],
        "wind": report["currentConditions"]["wind"]["speed"]["text"] + " km/h"
    }
    print(json.dumps(data))


def query_prov(coll, query, prov):
    print(f"Querying forecasts in {prov}...")
    data = [doc for doc in coll.find(query)]
    print(f"  Found {len(data)} documents")
    if data:
        print("Example:")
        idx = randint(0, len(data) - 1)
        print_doc_info(data[idx])
    input("Press enter to continue")
    print()


"""
Establish connection to router, and query for AB, ON, NB
"""
print_demo_title(1, "VERIFYING DATA IS IN DATABASE")

conn = TOR_ROUTER.connect(direct=False)
coll = conn \
    .get_database(DB_NAME) \
    .get_collection(COLLECTION_NAME)

query_prov(coll, AB_QUERY, "Alberta")
query_prov(coll, ON_QUERY, "Ontario")
query_prov(coll, NB_QUERY, "New Brunswick")

conn.close()

"""
Establish direct connection to MB replica set, and query for AB, ON, NB
- print first found
"""
print_demo_title(2, "VERIFYING MANITOBA CLUSTER CONTAINS CORRECT DATA")
print("Establishing direct connection to the MANITOBA replica set")
conn = MB_REPL_WIN.connect()
coll = conn \
    .get_database(DB_NAME) \
    .get_collection(COLLECTION_NAME)

query_prov(coll, AB_QUERY, "Alberta")
query_prov(coll, ON_QUERY, "Ontario")
query_prov(coll, NB_QUERY, "New Brunswick")

conn.close()

"""
Establish direct connection to ON replica set, and query for AB, ON, NB
- print first found
"""
print_demo_title(3, "VERIFYING ONTARIO CLUSTER CONTAINS CORRECT DATA")
print("Establishing direct connection to the ONTARIO replica set")
conn = ON_REPL_TOR.connect()
coll = conn \
    .get_database(DB_NAME) \
    .get_collection(COLLECTION_NAME)

query_prov(coll, AB_QUERY, "Alberta")
query_prov(coll, ON_QUERY, "Ontario")
query_prov(coll, NB_QUERY, "New Brunswick")

conn.close()

"""
Establish direct connection to QC replica set, and query for AB, ON, NB
- print first found
"""
print_demo_title(4, "VERIFYING QUEBEC CLUSTER CONTAINS CORRECT DATA")
print("Establishing direct connection to the QUEBEC replica set")
conn = QC_REPL_MON.connect()
coll = conn \
    .get_database(DB_NAME) \
    .get_collection(COLLECTION_NAME)

query_prov(coll, AB_QUERY, "Alberta")
query_prov(coll, ON_QUERY, "Ontario")
query_prov(coll, NB_QUERY, "New Brunswick")

conn.close()