import time

from base.db_physical import MongoConfigServer, MongoShardServer, DataCentre, \
    BaseMongoServer


class ConfigServerReplicaSet:
    pref_primary: MongoConfigServer
    config_servers: list[MongoConfigServer] = []
    replica_set_name: str

    def __init__(self, replica_set_name: str):
        self.replica_set_name = replica_set_name

    def wait_until_healthy(self):
        servers: list[BaseMongoServer] = self.get_servers()
        for server in servers:
            while not server.healthy():
                time.sleep(1)

    def add_server(
            self, dc: DataCentre, pref_primary: bool = False
    ) -> MongoConfigServer:
        server: MongoConfigServer = dc.add_config_server(
            replica_set_name=self.replica_set_name
        )

        self.config_servers.append(server)
        if pref_primary:
            self.pref_primary = server

        return server

    def get_servers(self) -> list[BaseMongoServer]:
        return self.config_servers

    def initiate_replica_set(self) -> dict:
        return self.pref_primary.initiate_replica_set(
            members=[
                node for node in self.config_servers
                if node.container_name != self.pref_primary.container_name
            ]
        )


class ShardServerReplicaSet:
    pref_primary: MongoShardServer
    replica_set_name: str
    shard_servers: list[MongoShardServer]

    def __init__(self, replica_set_name: str):
        self.replica_set_name = replica_set_name
        self.shard_servers = []

    def wait_until_healthy(self):
        servers: list[BaseMongoServer] = self.get_servers()
        for server in servers:
            while not server.healthy():
                time.sleep(1)

    def add_server(
            self, dc: DataCentre, pref_primary: bool = False
    ) -> MongoShardServer:
        server: MongoShardServer = dc.add_shard_server(
            replica_set_name=self.replica_set_name
        )

        self.shard_servers.append(server)
        if pref_primary:
            self.pref_primary = server

        return server

    def get_servers(self) -> list[BaseMongoServer]:
        return self.shard_servers

    def initiate_replica_set(self) -> dict:
        data: dict = self.pref_primary.initiate_replica_set(
            members=[
                node for node in self.shard_servers
                if node.container_name != self.pref_primary.container_name
            ]
        )

