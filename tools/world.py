#!/usr/bin/env python3
"""The night of the final, as the platform's systems would see it.

Three cities, each a 3 x 3 grid of zones two kilometres on a side. Riders
ride around them and report their position every twelve seconds, except when
they stop to watch a goal or the penalties. Customers
order from restaurants, more and more as the match goes on, most of all at
half-time, and each order moves through its life: placed, accepted, picked up,
then delivered or cancelled. Everything is drawn from one seeded generator,
so every run sees the same night and tools/reference.py can say exactly what
the pipeline must produce.

    python3 tools/world.py --out state/world
    python3 tools/world.py --out state/world --cells 12 --riders-per-cell 90

A bigger night is the same night in more cities: --cells adds cities (named
city-04 onwards), and the per-city sizes scale with their flags. Every size
is recorded in manifest.json, which the feeders and the model read.

Writes cells.json, restaurants.jsonl, customers.jsonl, riders.jsonl,
orders.jsonl (every order with every time in its life), changes.jsonl (the
order store's changes in time order, what tools/feed_orders.py applies),
pings.jsonl (every position report in time order, what tools/feed_pings.py
sends) and manifest.json. The constants below are shared with the reference
model, which imports this file.
"""
import argparse
import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path

SEED = 7
MIN = 60_000
# Kick-off, in epoch milliseconds. Every time in the world is relative to it.
T0 = int(datetime(2026, 7, 19, 19, 0, tzinfo=timezone.utc).timestamp() * 1000)
START_MIN = -20        # the first orders and pings, before kick-off
LAST_ORDER_MIN = 135   # orders stop here
PINGS_UNTIL_MIN = 160  # riders keep riding, so every window closes

CELLS = ["lisbon", "nairobi", "osaka"]
GRID = 3
ZONE_M = 2000
CELL_M = GRID * ZONE_M
RIDERS_PER_CELL = 72
RESTAURANTS_PER_CELL = 12
CUSTOMERS_PER_CELL = 400
PING_EVERY_MS = 12_000
BASE_ORDERS_PER_MIN = 12.0  # per city, before the phase multiplier

# The match, minute by minute: how many more or fewer people order.
PHASES = [
    (-20, 0, "before kick-off", 0.6),
    (0, 45, "the first half", 0.5),
    (45, 60, "half-time", 3.0),
    (60, 90, "the second half", 0.6),
    (90, 120, "extra time", 0.9),
    (120, 125, "penalties", 0.3),
    (125, 135, "after the whistle", 1.5),
]
GOALS = [23, 67, 88, 104]  # orders surge for two minutes after each goal
GOAL_BOOST = 2.5
# Riders watch the match too: half of them stop for a while after a goal or
# for the penalties, and report nothing until they ride again.
BREAK_SHARE = 0.5
BREAK_STARTS = GOALS + [120]
BREAK_MIN = (6, 12)

# What the pipeline computes. tools/reference.py uses the same functions.
HOP_SIZE_MS = 5 * MIN
HOP_SLIDE_MS = 1 * MIN
DISPATCH_BEFORE_MS = 3 * MIN  # a rider's position counts from this long before the order
DISPATCH_AFTER_MS = 2 * MIN   # until this long after it
DISPATCH_RADIUS_M = 1500      # and only within this distance of the restaurant
SCORE_SCALE = 100_000         # score = squared distance * SCORE_SCALE + rider id
PROMISE_MS = 35 * MIN
LATE_GRACE_MS = 10 * MIN
LATE_CREDIT_PCT = 20
CANCEL_SHARE = 0.04
RIDER_SPEED_M_S = 4.0
HOME_RANGE_M = 1200  # a rider further than this from their restaurant heads back

# The kitchens and the menus (round 2), drawn apart from the streams above so
# those stay as they were.
MENU_ITEMS = 8                      # dishes on each menu
MENU_CHANGES = (3, 7)               # price moves and sell-outs per restaurant through the night
BOUNCE_RUN = [1, 2, 3, 3, 4, 5, 6]  # requests an overloaded kitchen declines in a row
BOUNCE_SHARE = 0.5                  # the chance a busy kitchen does, in a busy minute
BOUNCE_FROM = 1.5                   # a kitchen is overloaded from this phase multiplier
ALERT_RUN = 3                       # consecutive bounces that raise an alert
GOAL_SURGE_MS = 2 * MIN             # the orders placed this long after a goal are its surge
KITCHEN_LAG_MS = 1 * MIN            # the tablet bus's lateness allowance (pipeline/kitchen.sql)
DISHES = ["margherita", "pad thai", "bibimbap", "shawarma", "ramen", "pho", "jollof rice",
          "nyama choma", "bacalhau", "francesinha", "okonomiyaki", "takoyaki", "biryani",
          "falafel wrap", "poke bowl", "lamb tagine", "pierogi", "arepas", "ceviche",
          "katsu curry", "moussaka", "injera platter", "pastel de nata", "mochi"]


def phase_multiplier(minute):
    m = 0.0
    for lo, hi, _, mult in PHASES:
        if lo <= minute < hi:
            m = mult
    for g in GOALS:
        if g <= minute < g + 2:
            m *= GOAL_BOOST
    return m


def surge_pct(demand, supply):
    """The multiplier a zone's pricing window gives, as pipeline/pricing.sql computes it."""
    if supply == 0:
        return 200
    ratio = demand * 100 // supply
    if ratio >= 300:
        return 180
    if ratio >= 200:
        return 150
    if ratio >= 100:
        return 120
    return 100


def zone_of(cell, x, y):
    return f"{cell}-z{y // ZONE_M}{x // ZONE_M}"


def zone_touches(cell):
    """Each zone and the zones it shares an edge with."""
    out = {}
    for r in range(GRID):
        for c in range(GRID):
            near = []
            for dr, dc in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                rr, cc = r + dr, c + dc
                if 0 <= rr < GRID and 0 <= cc < GRID:
                    near.append(f"{cell}-z{rr}{cc}")
            out[f"{cell}-z{r}{c}"] = near
    return out


def fee_cents(dist_m):
    return 199 + 20 * (dist_m // 100)


def dist_m(x1, y1, x2, y2):
    return int(math.isqrt((x1 - x2) ** 2 + (y1 - y2) ** 2))


def poisson(rng, lam):
    if lam <= 0:
        return 0
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def build(rng):
    cells = {c: {"zones": zone_touches(c)} for c in CELLS}
    restaurants, customers, riders = [], [], []
    for ci, cell in enumerate(CELLS):
        for i in range(RESTAURANTS_PER_CELL):
            x, y = rng.randrange(200, CELL_M - 200), rng.randrange(200, CELL_M - 200)
            restaurants.append({"restaurant_id": f"{cell}-r{i:02d}", "cell": cell,
                                "zone": zone_of(cell, x, y), "x": x, "y": y,
                                "prep_min": rng.randint(8, 18),
                                "weight": rng.choice([1, 1, 2, 3, 5])})
        for i in range(CUSTOMERS_PER_CELL):
            x, y = rng.randrange(0, CELL_M), rng.randrange(0, CELL_M)
            customers.append({"customer_id": f"{cell}-c{i:04d}", "cell": cell,
                              "zone": zone_of(cell, x, y), "x": x, "y": y})
        for i in range(RIDERS_PER_CELL):
            # Riders work where the orders are: each keeps near a restaurant,
            # the busier the likelier, so a quiet restaurant sees few riders.
            mine = restaurants[-RESTAURANTS_PER_CELL:]
            home = rng.choices(mine, [r["weight"] ** 2 for r in mine])[0]
            rider = {"rider_id": (ci + 1) * 1000 + i + 1, "cell": cell,
                     "x": min(max(home["x"] + rng.randint(-800, 800), 0), CELL_M - 1),
                     "y": min(max(home["y"] + rng.randint(-800, 800), 0), CELL_M - 1),
                     "home_x": home["x"], "home_y": home["y"],
                     "heading": rng.uniform(0, 2 * math.pi),
                     "speed": rng.uniform(3.0, 6.0),
                     "offset_ms": rng.randrange(0, PING_EVERY_MS),
                     "break_from_ms": None, "break_until_ms": None}
            if rng.random() < BREAK_SHARE:
                start = T0 + rng.choice(BREAK_STARTS) * MIN + rng.randrange(0, 2 * MIN)
                rider["break_from_ms"] = start
                rider["break_until_ms"] = start + rng.randint(*BREAK_MIN) * MIN
            riders.append(rider)
    return cells, restaurants, customers, riders


def ride(rng, riders):
    """Every position report, in time order."""
    pings = []
    start, end = T0 + START_MIN * MIN, T0 + PINGS_UNTIL_MIN * MIN
    for r in riders:
        x, y, heading, speed = float(r["x"]), float(r["y"]), r["heading"], r["speed"]
        t = start + r["offset_ms"]
        while t < end:
            resting = r["break_from_ms"] is not None and r["break_from_ms"] <= t < r["break_until_ms"]
            if not resting:
                pings.append({"ts": t, "rider_id": r["rider_id"], "cell": r["cell"],
                              "zone": zone_of(r["cell"], int(x), int(y)), "x": int(x), "y": int(y)})
            step = speed * PING_EVERY_MS / 1000.0
            x += step * math.cos(heading)
            y += step * math.sin(heading)
            if not 0 <= x < CELL_M:
                x = min(max(x, 0.0), CELL_M - 1.0)
                heading = math.pi - heading
            if not 0 <= y < CELL_M:
                y = min(max(y, 0.0), CELL_M - 1.0)
                heading = -heading
            heading += rng.gauss(0, 0.4)
            if dist_m(int(x), int(y), r["home_x"], r["home_y"]) > HOME_RANGE_M:
                heading = math.atan2(r["home_y"] - y, r["home_x"] - x) + rng.gauss(0, 0.3)
            t += PING_EVERY_MS
    pings.sort(key=lambda p: (p["ts"], p["rider_id"]))
    return pings


def order_book(rng, restaurants, customers):
    """Every order, with every time in its life."""
    by_cell_r = {c: [r for r in restaurants if r["cell"] == c] for c in CELLS}
    by_cell_c = {c: [k for k in customers if k["cell"] == c] for c in CELLS}
    orders = []
    n = 0
    for cell in CELLS:
        rs, ks = by_cell_r[cell], by_cell_c[cell]
        weights = [r["weight"] for r in rs]
        for minute in range(START_MIN, LAST_ORDER_MIN):
            busy = phase_multiplier(minute)
            for _ in range(poisson(rng, BASE_ORDERS_PER_MIN * busy)):
                n += 1
                r = rng.choices(rs, weights)[0]
                k = rng.choice(ks)
                # Never on a minute boundary: a pricing window ends there.
                placed = T0 + minute * MIN + rng.randrange(1, MIN)
                d = dist_m(r["x"], r["y"], k["x"], k["y"])
                accepted = placed + rng.randint(30_000, 180_000)
                prep = int(r["prep_min"] * MIN * (1 + 0.5 * (busy - 0.5)))
                picked_up = accepted + prep
                travel = int(d / RIDER_SPEED_M_S * 1000 * (1 + 0.3 * (busy - 0.5)))
                delivered = picked_up + travel
                o = {"order_id": f"{cell}-o{n:05d}", "cell": cell, "zone": k["zone"],
                     "restaurant_id": r["restaurant_id"], "customer_id": k["customer_id"],
                     "cx": k["x"], "cy": k["y"], "placed_at": placed,
                     "promised_at": placed + PROMISE_MS, "fee_cents": fee_cents(d),
                     "total_cents": rng.randint(800, 4500), "accepted_at": accepted,
                     "picked_up_at": picked_up, "delivered_at": delivered, "cancelled_at": None}
                if rng.random() < CANCEL_SHARE:
                    o["cancelled_at"] = placed + rng.randint(MIN, 10 * MIN)
                    o["picked_up_at"] = o["delivered_at"] = None
                    if o["cancelled_at"] <= accepted:
                        o["accepted_at"] = None
                orders.append(o)
    orders.sort(key=lambda o: (o["placed_at"], o["order_id"]))
    return orders


def changes_of(order):
    """The order store's changes for one order, in time order."""
    row = {k: order[k] for k in ("order_id", "cell", "zone", "restaurant_id", "customer_id",
                                 "cx", "cy", "placed_at", "promised_at", "fee_cents",
                                 "total_cents")}
    out = [dict(row, op="insert", status="placed", updated_at=order["placed_at"])]
    for status, key in (("accepted", "accepted_at"), ("picked_up", "picked_up_at"),
                        ("delivered", "delivered_at"), ("cancelled", "cancelled_at")):
        if order.get(key) is not None:
            out.append(dict(row, op="update", status=status, updated_at=order[key]))
    for i in range(1, len(out)):  # strictly later than the change before
        if out[i]["updated_at"] <= out[i - 1]["updated_at"]:
            out[i]["updated_at"] = out[i - 1]["updated_at"] + 1
    return out


def menus(rng, restaurants):
    """Every restaurant's menu as the legacy system holds it before the night,
    and the changes made to it during the night: a price moved, a dish sold out
    or back on."""
    items, changes = [], []
    opened = T0 + START_MIN * MIN - 60 * MIN
    for r in restaurants:
        menu = []
        for j, name in enumerate(rng.sample(DISHES, MENU_ITEMS)):
            item = {"restaurant_id": r["restaurant_id"], "item_id": f"i{j:02d}", "name": name,
                    "price_cents": rng.randrange(400, 2600, 50), "available": 1, "updated_at": opened}
            items.append(item)
            menu.append(dict(item))
        # Never before kick-off: the change stream's snapshot is taken as the
        # jobs start, and a change made before it would be part of the snapshot.
        for minute in sorted(rng.randrange(0, LAST_ORDER_MIN) for _ in range(rng.randint(*MENU_CHANGES))):
            item = rng.choice(menu)
            if rng.random() < 0.4:
                item["available"] = 1 - item["available"]
            else:
                item["price_cents"] = max(200, item["price_cents"] + rng.choice([-100, -50, 50, 100, 150, 200]))
            item["updated_at"] = T0 + minute * MIN + rng.randrange(0, MIN)
            changes.append(dict(item))
    changes.sort(key=lambda c: (c["updated_at"], c["restaurant_id"], c["item_id"]))
    last = {}
    for c in changes:  # strictly later than the change before to the same item
        key = (c["restaurant_id"], c["item_id"])
        if c["updated_at"] <= last.get(key, -1):
            c["updated_at"] = last[key] + 1
        last[key] = c["updated_at"]
    return items, changes


def kitchen(rng, orders, restaurants):
    """The restaurant tablets' events: each order accepted and ready, and, when a
    kitchen is overloaded, requests declined in a row, which the platform never
    turned into orders."""
    by_id = {r["restaurant_id"]: r for r in restaurants}
    events = []

    def add(order_id, r, status, ts):
        events.append({"event_id": f"k{len(events) + 1:06d}", "order_id": order_id,
                       "restaurant_id": r["restaurant_id"], "cell": r["cell"], "status": status, "ts": ts})

    for o in orders:
        r = by_id[o["restaurant_id"]]
        if o["accepted_at"] is not None:
            add(o["order_id"], r, "accepted", o["accepted_at"] + rng.randrange(0, 1500))
        if o["picked_up_at"] is not None:
            add(o["order_id"], r, "ready", o["picked_up_at"] - rng.randrange(10_000, 90_000))
    n = 0
    for r in restaurants:
        if r["weight"] < 3:
            continue
        for minute in range(START_MIN, LAST_ORDER_MIN):
            if phase_multiplier(minute) < BOUNCE_FROM or rng.random() >= BOUNCE_SHARE:
                continue
            ts = T0 + minute * MIN + rng.randrange(0, MIN)
            for _ in range(rng.choice(BOUNCE_RUN)):
                n += 1
                add(f"{r['cell']}-q{n:05d}", r, "bounced", ts)
                ts += rng.randrange(5_000, 25_000)
    # Strictly later than the event before at the same restaurant, so the
    # order the pattern sees is the order the model sees.
    events.sort(key=lambda e: (e["restaurant_id"], e["ts"], e["event_id"]))
    last = {}
    for e in events:
        if e["ts"] <= last.get(e["restaurant_id"], -1):
            e["ts"] = last[e["restaurant_id"]] + 1
        last[e["restaurant_id"]] = e["ts"]
    events.sort(key=lambda e: (e["ts"], e["event_id"]))
    return events


def match_events():
    """The broadcaster's feed: what happened on the pitch, when. A goal's time
    is the start of its minute, which is when the orders surge."""
    out = []
    kinds = {0: ("kick_off", "the first half"), 45: ("half_time", "the break"), 60: ("second_half", "play resumes"),
             90: ("extra_time", "still level"), 120: ("penalties", "a shoot-out"), 125: ("final_whistle", "it is over")}
    score = [0, 0]
    for minute in sorted(set(kinds) | set(GOALS)):
        if minute in GOALS:
            score[len(out) % 2] += 1
            out.append({"minute": minute, "kind": "goal", "detail": f"{score[0]}-{score[1]}"})
        if minute in kinds:
            kind, detail = kinds[minute]
            out.append({"minute": minute, "kind": kind, "detail": detail})
    for i, e in enumerate(out, start=1):
        e["seq"] = i
        e["ts"] = T0 + e["minute"] * MIN
    return out


def write_jsonl(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")


def main():
    global CELLS, RIDERS_PER_CELL, RESTAURANTS_PER_CELL, CUSTOMERS_PER_CELL, BASE_ORDERS_PER_MIN
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default="state/world")
    ap.add_argument("--cells", type=int, default=len(CELLS),
                    help="how many cities (3 by default; more are named city-04 onwards)")
    ap.add_argument("--riders-per-cell", type=int, default=RIDERS_PER_CELL)
    ap.add_argument("--restaurants-per-cell", type=int, default=RESTAURANTS_PER_CELL)
    ap.add_argument("--customers-per-cell", type=int, default=CUSTOMERS_PER_CELL)
    ap.add_argument("--orders-per-minute", type=float, default=BASE_ORDERS_PER_MIN,
                    help="per city, before the phase multiplier")
    args = ap.parse_args()
    if args.cells != len(CELLS):
        CELLS = (CELLS + [f"city-{i:02d}" for i in range(len(CELLS) + 1, args.cells + 1)])[:args.cells]
    RIDERS_PER_CELL, RESTAURANTS_PER_CELL = args.riders_per_cell, args.restaurants_per_cell
    CUSTOMERS_PER_CELL, BASE_ORDERS_PER_MIN = args.customers_per_cell, args.orders_per_minute
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    cells, restaurants, customers, riders = build(rng)
    pings = ride(rng, riders)
    orders = order_book(rng, restaurants, customers)
    changes = [c for o in orders for c in changes_of(o)]
    changes.sort(key=lambda c: (c["updated_at"], c["order_id"]))
    rng2 = random.Random(SEED + 1)
    items, menu_changes = menus(rng2, restaurants)
    tablet = kitchen(rng2, orders, restaurants)
    (out / "cells.json").write_text(json.dumps(cells, indent=1))
    write_jsonl(out / "restaurants.jsonl", restaurants)
    write_jsonl(out / "customers.jsonl", customers)
    write_jsonl(out / "riders.jsonl", riders)
    write_jsonl(out / "orders.jsonl", orders)
    write_jsonl(out / "changes.jsonl", changes)
    write_jsonl(out / "pings.jsonl", pings)
    write_jsonl(out / "menus.jsonl", items)
    write_jsonl(out / "menu_changes.jsonl", menu_changes)
    write_jsonl(out / "kitchen.jsonl", tablet)
    write_jsonl(out / "match.jsonl", match_events())
    cancelled = sum(1 for o in orders if o["cancelled_at"] is not None)
    bounced = sum(1 for e in tablet if e["status"] == "bounced")
    manifest = {"seed": SEED, "t0_ms": T0, "cells": CELLS, "phases": PHASES, "goals": GOALS,
                "restaurants": len(restaurants), "customers": len(customers),
                "riders": len(riders), "orders": len(orders), "cancelled": cancelled,
                "changes": len(changes), "pings": len(pings),
                "menu_items": len(items), "menu_changes": len(menu_changes),
                "kitchen_events": len(tablet), "bounces": bounced,
                "first_ms": T0 + START_MIN * MIN, "last_order_ms": orders[-1]["placed_at"],
                "pings_until_ms": T0 + PINGS_UNTIL_MIN * MIN}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1))
    per_phase = {name: sum(1 for o in orders if lo <= (o["placed_at"] - T0) // MIN < hi)
                 for lo, hi, name, _ in PHASES}
    print(f"world: {len(CELLS)} cities, {len(restaurants)} restaurants, {len(riders)} riders, "
          f"{len(customers)} customers -> {len(orders)} orders ({cancelled} cancelled), "
          f"{len(changes)} order changes, {len(pings)} position reports")
    print("orders by phase: " + ", ".join(f"{k} {v}" for k, v in per_phase.items()))
    print(f"the kitchens: {len(items)} menu items, {len(menu_changes)} menu changes, "
          f"{len(tablet)} tablet events of which {bounced} bounces")


if __name__ == "__main__":
    main()
