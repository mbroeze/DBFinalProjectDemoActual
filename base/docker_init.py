from docker import from_env as docker_from_env
from docker import DockerClient
from docker.errors import APIError
from docker.models.networks import Network

DOCKER_CLIENT: DockerClient = docker_from_env()

# create network
DOCKER_NETWORK_NAME: str = "the-internet-(db-proj)"
DOCKER_NETWORK: Network
try:
    DOCKER_NETWORK = DOCKER_CLIENT.networks.create(
        name=DOCKER_NETWORK_NAME,
        check_duplicate=True
    )
except APIError:
    DOCKER_NETWORK = DOCKER_CLIENT.networks.list(names=[DOCKER_NETWORK_NAME])[0]

