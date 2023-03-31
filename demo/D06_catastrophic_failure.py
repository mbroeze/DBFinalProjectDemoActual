import time

from demo.D00_init_server_setup import TOR_ROUTER, TOR_CFG, ON_REPL_TOR
from demo.D03_scaling_server_setup import MB_REPL_TOR, QC_REPL_TOR
from demo.simple_data import print_demo_title

print()
print_demo_title(1, "DISASTER HAS STRUCK TORONTO")
print()

servers = [
    TOR_ROUTER,
    TOR_CFG,
    MB_REPL_TOR,
    ON_REPL_TOR,
    QC_REPL_TOR
]

for server in servers:
    print(f"Shutting down {server.container_name}...")
    server.shutdown()
print()

for server in servers:
    while server.healthy():
        time.sleep(1)
print()

print("Toronto is down!")
input("Press enter to continue")