import pymongo
from docker.errors import NotFound
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, NetworkTimeout

from base.docker_init import DOCKER_CLIENT, DOCKER_NETWORK
from docker.models.containers import Container


class BaseMongoServer:
    image: str = "mongo"
    container_name: str
    external_port: int
    internal_port: int = 27017
    internal_datapath: str = "/data/db"
    has_data_volume: bool

    data_volume_name: str | None = None

    container_commands: list[tuple[str, str]]

    container: Container

    def __init__(
            self,
            container_name: str,
            external_port: int,
            has_data_volume: bool,
            container_commands: list[tuple[str, str]]
    ):
        self.container_name = container_name
        self.external_port = external_port
        self.has_data_volume = has_data_volume
        if has_data_volume:
            self.data_volume_name = container_name

        self.container_commands = container_commands

        try:
            self.container = DOCKER_CLIENT.containers.get(self.container_name)
        except NotFound:
            self.create()
        self.container.start()

    def _build_volume_list(self) -> list[str]:
        volume_list: list[str] = [
            "/etc/localtime:/etc/localtime:ro"
        ]
        if self.has_data_volume:
            volume_list.append(
                f"{self.data_volume_name}:{self.internal_datapath}"
            )
        return volume_list

    def _build_container_command(self) -> str:
        return " ".join([
            f"{cmd[0]} {cmd[1]}"
            for cmd in self.container_commands
        ])

    def create(self):
        self.container = DOCKER_CLIENT.containers.create(
            image=self.image,
            command=self._build_container_command(),
            name=self.container_name,
            network=DOCKER_NETWORK.name,
            environment={"TERM": "xterm"},
            ports={self.internal_port: self.external_port},
            volumes=self._build_volume_list(),
            detach=True,
        )

    def shutdown(self):
        self.container.stop()

    def startup(self):
        self.container.start()

    def destroy(self):
        self.container.remove(force=True)
        DOCKER_CLIENT.volumes.get(self.data_volume_name).remove()

    def healthy(self) -> bool:
        self.container.reload()

        healthy_status: list[str] = ["running", "created"]
        container_healthy: bool = self.container.status in healthy_status
        mongo_healthy: bool = False
        if container_healthy:
            conn = self.connect()
            try:
                with pymongo.timeout(2):
                    mongo_ping: dict = conn.admin.command("ping")
                    mongo_healthy = mongo_ping["ok"] == 1.0
            except (ServerSelectionTimeoutError, NetworkTimeout) as err:
                mongo_healthy = False
            finally:
                conn.close()

        return container_healthy and mongo_healthy

    def connect(self, direct: bool = True) -> MongoClient:
        return MongoClient(
            "localhost",
            self.external_port,
            directConnection=direct,
        )


class MongoShardServer(BaseMongoServer):
    replica_set_name: str

    def __init__(
            self,
            name: str,
            port: int,
            replica_set_name: str
    ):
        super().__init__(
            container_name=name,
            external_port=port,
            has_data_volume=True,
            container_commands=[
                ("mongod", ""),
                ("--shardsvr", ""),
                ("--replSet", replica_set_name),
                ("--dbpath", self.internal_datapath),
                ("--port", self.internal_port),
            ]
        )
        self.replica_set_name = replica_set_name

    def initiate_replica_set(self, members: list[BaseMongoServer]) -> dict:
        cmd: str = "replSetInitiate"
        members = [self, *members]
        arg: dict = {
            "_id": self.replica_set_name,
            "members": [
                {
                    "_id": idx,
                    "host": member.container_name,
                    "priority": 1 if idx == 0 else 0.5
                }
                for idx, member in enumerate(members)
            ]
        }
        print(arg)
        conn = self.connect()
        result = conn.admin.command(cmd, arg)
        conn.close()
        return result


class MongoConfigServer(BaseMongoServer):
    replica_set_name: str

    def __init__(
            self,
            name: str,
            port: int,
            replica_set_name: str
    ):
        super().__init__(
            container_name=name,
            external_port=port,
            has_data_volume=True,
            container_commands=[
                ("mongod", ""),
                ("--configsvr", ""),
                ("--replSet", replica_set_name),
                ("--dbpath", self.internal_datapath),
                ("--port", self.internal_port),
            ]
        )
        self.replica_set_name = replica_set_name

    def initiate_replica_set(self, members: list[BaseMongoServer]) -> dict:
        cmd: str = "replSetInitiate"
        members = [self, *members]
        arg: dict = {
            "_id": self.replica_set_name,
            'configsvr': True,
            "members": [
                {
                    "_id": idx,
                    "host": member.container_name,
                    "priority": 1 if idx == 0 else 0.5
                }
                for idx, member in enumerate(members)
            ]
        }
        conn = self.connect()
        result = conn.admin.command(cmd, arg)
        conn.close()
        return result


class MongoRouter(BaseMongoServer):
    def __init__(
            self,
            name: str,
            port: int,
            config_servers: list[MongoConfigServer]
    ):
        super().__init__(
            container_name=name,
            external_port=port,
            has_data_volume=False,
            container_commands=[
                ("mongos", ""),
                (
                    "--configdb",
                    f"{config_servers[0].replica_set_name}/"
                    f"{','.join([f'{svr.container_name}:{svr.internal_port}' for svr in config_servers])}"
                ),
                ("--port", self.internal_port),
                ("--bind_ip_all", "")
            ]
        )

    def add_shard(self, shard_server: MongoShardServer) -> dict:
        cmd: str = "addShard"
        arg: str = f"{shard_server.replica_set_name}" \
                   f"/{shard_server.container_name}:{shard_server.internal_port}"
        conn = self.connect()
        result = conn.admin.command(cmd, arg)
        conn.close()
        return result

    def has_shard(self, shard_server: MongoShardServer) -> bool:
        cmd: str = "listShards"
        arg: int = 1
        conn = self.connect()
        shard_data: dict = conn.admin.command(cmd, arg)
        conn.close()
        curr_shard_names: list[str] = [
            shard["_id"]
            for shard in shard_data["shards"]
        ]
        return shard_server.replica_set_name in curr_shard_names

    def mongo_url(self) -> str:
        return f"mongodb://localhost:{self.external_port}"


class DataCentre:
    location: str

    routers: list[MongoRouter] = []
    config_servers: list[MongoConfigServer] = []
    shard_servers: list[MongoShardServer] = []

    next_port: int

    def __init__(self, location: str, start_port: int):
        self.location = location
        self.next_port = start_port

    def _get_next_port(self) -> int:
        port: int = self.next_port
        self.next_port += 1
        return port

    def add_router(
            self, config_servers: list[MongoConfigServer]
    ) -> MongoRouter:
        dc: str = self.location.upper()
        stype: str = "ROUTER"
        sid: int = len(self.routers)

        name: str = f"dc-{dc}_type-{stype}_dcid-{sid}"
        port: int = self._get_next_port()

        router: MongoRouter = MongoRouter(
            name=name, port=port, config_servers=config_servers
        )
        self.routers.append(router)
        return router

    def add_config_server(
            self, replica_set_name: str
    ) -> MongoConfigServer:
        dc: str = self.location.upper()
        repl_set: str = replica_set_name.upper()
        stype: str = "CONFIG"
        sid: int = len(self.config_servers)

        name: str = f"dc-{dc}_type-{stype}_replSet-{repl_set}_dcid-{sid}"
        port: int = self._get_next_port()

        config_server: MongoConfigServer = MongoConfigServer(
            name=name, port=port, replica_set_name=replica_set_name
        )
        self.config_servers.append(config_server)
        return config_server

    def add_shard_server(
            self, replica_set_name: str
    ) -> MongoShardServer:
        dc: str = self.location.upper()
        repl_set: str = replica_set_name.upper()
        stype: str = "SHARD"
        sid: int = len(self.shard_servers)

        name: str = f"dc-{dc}_type-{stype}_replSet-{repl_set}_dcid-{sid}"
        port: int = self._get_next_port()

        shard_server: MongoShardServer = MongoShardServer(
            name=name, port=port, replica_set_name=replica_set_name
        )

        self.shard_servers.append(shard_server)
        return shard_server
