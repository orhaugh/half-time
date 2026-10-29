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


# Round 2: the systems around the core.
#
#    HT_MYSQL           menu store        host:port   default mysql:3306
#    HT_PULSAR          tablet bus        host        default pulsar (ports 6650 and 8080)
#    HT_RABBITMQ_HTTP   desk queue        http://host:port  default http://localhost:${RABBITMQ_HTTP_PORT:-15672}
#    HT_NATS            alert bus         host:port   default nats:4222
#    HT_REDIS           tracker           host:port   default redis:6379
#    HT_CASSANDRA       order history     host:port   default cassandra:9042
MYSQL_IMAGE = "mysql:8.0"
PULSAR_IMAGE = "apachepulsar/pulsar:3.3.1"
NATS_BOX_IMAGE = "natsio/nats-box@sha256:dd57687c32199867bfc42823e935174f37754a4281de957d6717469948b5b18b"
REDIS_IMAGE = "redis:7"
CASSANDRA_IMAGE = "cassandra:4"


def _host_port(var, default):
    host, _, port = os.environ.get(var, default).rpartition(":")
    return host, port


def mysql(*args):
    """The argv of the mysql client against the menu store, as root."""
    if MODE == "rig":
        host, port = _host_port("HT_MYSQL", "mysql:3306")
        return ["docker", "run", "--rm", "-i", "--network", "host", "-e", "MYSQL_PWD=halftime", MYSQL_IMAGE,
                "mysql", "-h", host, "-P", port, "-u", "root", "halftime", *args]
    return ["docker", "compose", "exec", "-T", "-e", "MYSQL_PWD=halftime", "mysql",
            "mysql", "-u", "root", "halftime", *args]


def pulsar_client(seconds, *args):
    """The argv of pulsar-client, stopped after that many seconds: a consume runs
    until it is stopped."""
    if MODE == "rig":
        host = os.environ.get("HT_PULSAR", "pulsar")
        return ["docker", "run", "--rm", "-i", "--network", "host", PULSAR_IMAGE, "timeout", str(seconds),
                "bin/pulsar-client", "--url", f"pulsar://{host}:6650", *args]
    return ["docker", "compose", "exec", "-T", "pulsar", "timeout", str(seconds), "bin/pulsar-client", *args]


def pulsar_admin(*args):
    if MODE == "rig":
        host = os.environ.get("HT_PULSAR", "pulsar")
        return ["docker", "run", "--rm", "-i", "--network", "host", PULSAR_IMAGE,
                "bin/pulsar-admin", "--admin-url", f"http://{host}:8080", *args]
    return ["docker", "compose", "exec", "-T", "pulsar", "bin/pulsar-admin", *args]


def nats(*args):
    """The argv of the nats command line against the alert bus. On one machine
    it runs as a throwaway container of the nats-init service, on the compose
    network."""
    if MODE == "rig":
        return ["docker", "run", "--rm", "-i", "--network", "host", NATS_BOX_IMAGE,
                "nats", "--server", f"nats://{os.environ.get('HT_NATS', 'nats:4222')}", *args]
    return ["docker", "compose", "run", "--rm", "--no-deps", "-T", "--quiet-pull", "--entrypoint", "nats",
            "nats-init", "--server", "nats://nats:4222", *args]


def redis_cli(*args):
    if MODE == "rig":
        host, port = _host_port("HT_REDIS", "redis:6379")
        return ["docker", "run", "--rm", "-i", "--network", "host", REDIS_IMAGE,
                "redis-cli", "-h", host, "-p", port, *args]
    return ["docker", "compose", "exec", "-T", "redis", "redis-cli", *args]


def cqlsh(*args):
    if MODE == "rig":
        host, port = _host_port("HT_CASSANDRA", "cassandra:9042")
        return ["docker", "run", "--rm", "-i", "--network", "host", CASSANDRA_IMAGE, "cqlsh", host, port, *args]
    return ["docker", "compose", "exec", "-T", "cassandra", "cqlsh", *args]


def rabbitmq_http():
    """The management API's base URL, and the credentials as user:password."""
    url = os.environ.get("HT_RABBITMQ_HTTP", f"http://localhost:{os.environ.get('RABBITMQ_HTTP_PORT', '15672')}")
    return url, "halftime:halftime"


# Round 3: search, the lakehouse's readers, telemetry and the gauges. All
# reached over HTTP, so the same in both modes.
#
#    HT_OPENSEARCH      search            http://host:port  default http://localhost:${OPENSEARCH_HOST_PORT:-29200}
#    HT_INFLUXDB        telemetry         http://host:port  default http://localhost:${INFLUXDB_HOST_PORT:-28086}
#    HT_PUSHGATEWAY     gauges            http://host:port  default http://localhost:${PUSHGATEWAY_HOST_PORT:-29091}
INFLUX_ORG = "halftime"
INFLUX_BUCKET = "halftime"
INFLUX_TOKEN = "halftime-token"


def opensearch_url():
    return os.environ.get("HT_OPENSEARCH", f"http://localhost:{os.environ.get('OPENSEARCH_HOST_PORT', '29200')}")


def influxdb_url():
    return os.environ.get("HT_INFLUXDB", f"http://localhost:{os.environ.get('INFLUXDB_HOST_PORT', '28086')}")


def pushgateway_url():
    return os.environ.get("HT_PUSHGATEWAY", f"http://localhost:{os.environ.get('PUSHGATEWAY_HOST_PORT', '29091')}")
