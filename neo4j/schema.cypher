// The city graph: cities, their zones and which zones touch, and restaurants
// in their zones. tools/load_graph.py loads it from the generated world before
// the jobs start, and the dispatch job reads it with a lookup per order.
//
// The constraints are what the lookups seek on.
CREATE CONSTRAINT city_id IF NOT EXISTS FOR (c:City) REQUIRE c.cellId IS UNIQUE;
CREATE CONSTRAINT zone_id IF NOT EXISTS FOR (z:Zone) REQUIRE z.zoneId IS UNIQUE;
CREATE CONSTRAINT restaurant_id IF NOT EXISTS FOR (r:Restaurant) REQUIRE r.restaurantId IS UNIQUE;
