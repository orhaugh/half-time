"""Where the systems are, and how the tools reach them.

On one machine (the default, HT_MODE=compose) the broker, the order store and
the analytics store are containers of docker-compose.yml, and their command
line tools run inside them. On a rig (HT_MODE=rig) they are on other hosts:
the tools run the same images as throwaway containers on the ops host, with
host networking, against the addresses in the environment:

    HT_KAFKA        broker            host:port   default kafka:9092 inside the compose network
    HT_POSTGRES     order store       host:port   default postgres:5432
    HT_CLICKHOUSE   analytics store   host:port   default clickhouse:9000 (the native port)
    HT_NEO4J        city graph        http://host:port  default http://localhost:${NEO4J_HTTP_PORT:-7474}

The Neo4j Query API is HTTP, so it is addressed the same way in both modes.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODE = os.environ.get("HT_MODE", "compose")
KAFKA_IMAGE = "apache/kafka:3.9.1"
POSTGRES_IMAGE = "postgres:16.15"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.3"


def kafka_bootstrap():
    return os.environ.get("HT_KAFKA", "kafka:9092")


def kafka_tool(tool, *args):
    """The argv of a Kafka command line tool, run where it can reach the broker."""
    if MODE == "rig":
        return ["docker", "run", "--rm", "-i", "--network", "host", KAFKA_IMAGE,
                f"/opt/kafka/bin/{tool}", "--bootstrap-server", kafka_bootstrap(), *args]
    return ["docker", "compose", "exec", "-T", "kafka", f"/opt/kafka/bin/{tool}",
            "--bootstrap-server", kafka_bootstrap(), *args]


def psql(*args):
    """The argv of psql against the order store, as the halftime user."""
    if MODE == "rig":
        host, _, port = os.environ.get("HT_POSTGRES", "postgres:5432").rpartition(":")
        return ["docker", "run", "--rm", "-i", "--network", "host", "-e", "PGPASSWORD=halftime",
                POSTGRES_IMAGE, "psql", "-h", host, "-p", port, "-U", "halftime", "-d", "halftime", *args]
    return ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "halftime", "-d", "halftime", *args]


def clickhouse_client(*args):
    """The argv of clickhouse-client against the analytics store."""
    if MODE == "rig":
        host, _, port = os.environ.get("HT_CLICKHOUSE", "clickhouse:9000").rpartition(":")
        return ["docker", "run", "--rm", "-i", "--network", "host", CLICKHOUSE_IMAGE,
                "clickhouse-client", "--host", host, "--port", port,
                "--user", "halftime", "--password", "halftime", *args]
    return ["docker", "compose", "exec", "-T", "clickhouse", "clickhouse-client",
            "--user", "halftime", "--password", "halftime", *args]


def neo4j_url():
    return os.environ.get("HT_NEO4J", f"http://localhost:{os.environ.get('NEO4J_HTTP_PORT', '7474')}")
