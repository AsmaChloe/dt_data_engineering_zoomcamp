# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

From this MV, find the pair of taxi zones with the highest average trip time.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

```sql
CREATE MATERIALIZED VIEW trips_stats AS
with trip_data_time as ( 
    select *, 
    tpep_dropoff_datetime - tpep_pickup_datetime as trip_time
    from trip_data
)
select
    pu.zone as pu_zone, 
    do_.zone as do_zone,
    avg(trip_time) as avg_trip_time,
    min(trip_time) as min_trip_time,
    max(trip_time) as max_trip_time
from trip_data_time
inner join taxi_zone pu
on pu.location_id = pulocationid
inner join taxi_zone do_
on do_.location_id = dolocationid
group by 1, 2
;

select pu_zone, do_zone from trips_stats order by avg_trip_time desc limit 1;
```
Options:
**1. Yorkville East, Steinway**

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

```sql
CREATE MATERIALIZED VIEW trips_stats AS
with trip_data_time as ( 
    select *, 
    tpep_dropoff_datetime - tpep_pickup_datetime as trip_time
    from trip_data
)
select
    pu.zone as pu_zone, 
    do_.zone as do_zone,
    count(*) as trip_counts,
    avg(trip_time) as avg_trip_time,
    min(trip_time) as min_trip_time,
    max(trip_time) as max_trip_time
from trip_data_time
inner join taxi_zone pu
on pu.location_id = pulocationid
inner join taxi_zone do_
on do_.location_id = dolocationid
group by 1, 2
;

select trip_counts from trips_stats order by avg_trip_time desc limit 1;
```

Options:
**4. 1**

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00..

```sql
with latest_trip as (
    select max(tpep_pickup_datetime) as latest_pickup_datetime from trip_data
)
select 
    pu.zone,
    count(*) as trip_counts
from trip_data
cross join latest_trip
inner join taxi_zone pu
on pu.location_id = pulocationid
    and tpep_pickup_datetime between latest_trip.latest_pickup_datetime - interval '17' hour and latest_trip.latest_pickup_datetime 
group by 1
order by 2 desc
limit 3
;
```

Options:
**2. LaGuardia Airport, Lincoln Square East, JFK Airport**