ATTACH 'postgresql://postgres:postgres@postgres:5432/postgres' AS db (TYPE postgres);

CREATE TABLE db.green_aggregated_pulocationid (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pulocationid INTEGER,
    num_hits BIGINT,
    PRIMARY KEY (window_start, pulocationid)
);

CREATE TABLE db.green_aggregated_dolocationid (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    dolocationid INTEGER,
    num_hits BIGINT,
    PRIMARY KEY (window_start, dolocationid)
);

CREATE TABLE db.green_aggregated_both (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pulocationid INTEGER,
    dolocationid INTEGER,
    num_hits BIGINT,
    PRIMARY KEY (window_start, pulocationid, dolocationid)
);

CREATE TABLE zone_lookup AS SELECT * FROM 'data/taxi_zone_lookup.csv';

SELECT
    g.window_start,
    g.window_end,
    z.Zone,
    g.window_end - g.window_start AS duration
FROM db.green_aggregated_dolocationid g
LEFT JOIN zone_lookup z ON g.dolocationid = z.LocationID
ORDER BY duration DESC
LIMIT 10;

SELECT
    g.window_start,
    g.window_end,
    z.Zone,
    g.window_end - g.window_start AS duration
FROM db.green_aggregated_pulocationid g
LEFT JOIN zone_lookup z ON g.pulocationid = z.LocationID
ORDER BY duration DESC
LIMIT 10;

SELECT
    g.window_start,
    g.window_end,
    pu.Zone AS pickup_zone,
    dro.Zone AS dropoff_zone,
    g.window_end - g.window_start AS duration
FROM db.green_aggregated_both g
LEFT JOIN zone_lookup pu ON g.pulocationid = pu.LocationID
LEFT JOIN zone_lookup dro ON g.dolocationid = dro.LocationID
ORDER BY duration DESC
LIMIT 10;

CREATE TABLE db.raw (
    lpep_pickup_datetime VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    pulocationid INTEGER,
    dolocationid INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION
)
