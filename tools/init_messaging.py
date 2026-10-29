#!/usr/bin/env python3
"""What the messaging systems need before the jobs start.

    python3 tools/init_messaging.py

The operations desk's queue: the sink publishes to the default exchange with
the queue's name as the routing key, mandatory, so the queue has to exist or
the publish is returned and the flush fails. Pulsar's retention: the checks
read each topic back from its start, and Pulsar otherwise removes a message
once every subscription has acknowledged it. The alert bus's JetStream
stream is made by the nats-init service of docker-compose.yml.
"""
import base64
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosts  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def rabbitmq(method, path, body=None):
    url, auth = hosts.rabbitmq_http()
    req = urllib.request.Request(url + path, data=json.dumps(body).encode() if body is not None else None,
                                 method=method, headers={"Content-Type": "application/json",
                                                         "Authorization": "Basic " + base64.b64encode(auth.encode()).decode()})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
        return json.loads(data) if data else None


def main():
    rabbitmq("PUT", "/api/queues/%2F/ops.escalations", {"durable": True})
    print("desk: queue ops.escalations declared (RabbitMQ)")
    subprocess.run(hosts.pulsar_admin("namespaces", "set-retention", "public/default", "--size", "-1", "--time", "-1"),
                   cwd=ROOT, check=True, capture_output=True)
    print("tablet bus: public/default keeps every message (Pulsar)")


if __name__ == "__main__":
    main()
