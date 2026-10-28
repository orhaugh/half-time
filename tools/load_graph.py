#!/usr/bin/env python3
"""Load the city graph into Neo4j from the generated world, and check it.

    python3 tools/load_graph.py --reset      empty the graph, apply neo4j/schema.cypher, load
    python3 tools/load_graph.py --check      compare the graph with state/world

Cities, their zones with the zones each touches, and restaurants in their
zones: what pipeline/dispatch.sql looks up for every order.
"""
import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosts  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
PASSWORD = os.environ.get("NEO4J_PASSWORD", "half-time-pw")


def query(statement, parameters=None):
    body = json.dumps({"statement": statement, "parameters": parameters or {}}).encode()
    req = urllib.request.Request(
        f"{hosts.neo4j_url()}/db/neo4j/query/v2", body,
        {"Content-Type": "application/json",
         "Authorization": "Basic " + base64.b64encode(f"neo4j:{PASSWORD}".encode()).decode()})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            reply = json.load(r)
    except urllib.error.HTTPError as e:
        try:
            reply = json.load(e)
        except ValueError:
            raise SystemExit(f"load_graph: HTTP {e.code} from Neo4j") from None
    if reply.get("errors"):
        raise SystemExit(f"load_graph: {reply['errors'][0]['code']}: {reply['errors'][0]['message']}")
    return reply.get("data", {}).get("values", [])


def world(name):
    with open(ROOT / "state" / "world" / name) as f:
        return [json.loads(line) for line in f if line.strip()]


def load():
    cells = json.loads((ROOT / "state" / "world" / "cells.json").read_text())
    restaurants = world("restaurants.jsonl")
    query("MATCH (n) DETACH DELETE n")
    cypher = "\n".join(l for l in (ROOT / "neo4j" / "schema.cypher").read_text().splitlines()
                       if l.strip() and not l.strip().startswith("//"))
    for statement in cypher.split(";"):
        if statement.strip():
            query(statement.strip())
    zones, touches = [], []
    for cell, c in cells.items():
        for zone, near in c["zones"].items():
            zones.append({"id": zone, "cell": cell, "row": int(zone[-2]), "col": int(zone[-1])})
            touches.extend({"a": zone, "b": n} for n in near)
    query("UNWIND $cells AS c MERGE (:City {cellId: c})", {"cells": list(cells)})
    query("UNWIND $zones AS z MERGE (zn:Zone {zoneId: z.id}) "
          "SET zn.cell = z.cell, zn.row = z.row, zn.col = z.col "
          "WITH zn, z MATCH (c:City {cellId: z.cell}) MERGE (zn)-[:IN_CITY]->(c)", {"zones": zones})
    query("UNWIND $touches AS t MATCH (a:Zone {zoneId: t.a}), (b:Zone {zoneId: t.b}) "
          "MERGE (a)-[:TOUCHES]->(b)", {"touches": touches})
    query("UNWIND $rs AS r MERGE (x:Restaurant {restaurantId: r.restaurant_id}) "
          "SET x.cell = r.cell, x.x = r.x, x.y = r.y, x.prepMinutes = r.prep_min "
          "WITH x, r MATCH (z:Zone {zoneId: r.zone}) MERGE (x)-[:IN_ZONE]->(z)",
          {"rs": restaurants})
    return len(cells), len(zones), len(touches), len(restaurants)


def check():
    cells = json.loads((ROOT / "state" / "world" / "cells.json").read_text())
    restaurants = world("restaurants.jsonl")
    want = {"cities": len(cells),
            "zones": sum(len(c["zones"]) for c in cells.values()),
            "touches": sum(len(n) for c in cells.values() for n in c["zones"].values()),
            "restaurants": len(restaurants)}
    got = {"cities": query("MATCH (c:City) RETURN count(c)")[0][0],
           "zones": query("MATCH (z:Zone)-[:IN_CITY]->(:City) RETURN count(z)")[0][0],
           "touches": query("MATCH (:Zone)-[t:TOUCHES]->(:Zone) RETURN count(t)")[0][0],
           "restaurants": query("MATCH (r:Restaurant)-[:IN_ZONE]->(:Zone) RETURN count(r)")[0][0]}
    placed = {row[0]: row[1] for row in query(
        "MATCH (r:Restaurant)-[:IN_ZONE]->(z:Zone) RETURN r.restaurantId, z.zoneId")}
    wrong = [r["restaurant_id"] for r in restaurants if placed.get(r["restaurant_id"]) != r["zone"]]
    ok = got == want and not wrong
    print(f"  graph: {got['cities']} cities, {got['zones']} zones, {got['touches']} touches, "
          f"{got['restaurants']} restaurants; expected {want['cities']}, {want['zones']}, "
          f"{want['touches']}, {want['restaurants']}" + ("" if not wrong else
          f"; {len(wrong)} restaurants in the wrong zone") + ("  OK" if ok else "  FAIL"))
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    try:
        if args.reset:
            c, z, t, r = load()
            print(f"graph: emptied, neo4j/schema.cypher applied, {c} cities, {z} zones, "
                  f"{t} touches, {r} restaurants loaded")
        if args.check or not args.reset:
            sys.exit(0 if check() else 1)
    except urllib.error.URLError as e:
        raise SystemExit(f"load_graph: Neo4j did not answer: {e}")


if __name__ == "__main__":
    main()
