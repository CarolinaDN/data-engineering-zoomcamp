-- How many taxi trips were totally made on September 18th 2019?
-- 15612
SELECT COUNT(*)
FROM green_tripdata
WHERE
	CAST(lpep_pickup_datetime as DATE) = '2019-09-18' and CAST(lpep_dropoff_datetime as DATE) = '2019-09-18'


-- Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
-- 2019-09-26
SELECT "lpep_pickup_datetime"::date
FROM green_tripdata
GROUP BY "lpep_pickup_datetime"::date
ORDER by max(trip_distance) DESC
LIMIT 1

/* faster*/
SELECT "lpep_pickup_datetime"::date
FROM green_tripdata
ORDER by "trip_distance" DESC
LIMIT 1


-- Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
-- Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
-- "Brooklyn" "Manhattan" "Queens"
SELECT z."Borough"
FROM green_tripdata t JOIN zones z ON t."PULocationID" = z."LocationID"
WHERE
	"lpep_pickup_datetime"::date='2019-09-18' and
	z."Borough" != 'Unknown'
GROUP BY z."Borough"
HAVING sum(total_amount) > 50000


-- For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
-- We want the name of the zone, not the id.
SELECT zdo."Zone"
FROM green_tripdata t 
	JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
	EXTRACT('month' from  "lpep_pickup_datetime"::date) = 9
	and EXTRACT('year' from  "lpep_pickup_datetime"::date) = 2019
	and zpu."Zone"='Astoria'
ORDER by tip_amount DESC
LIMIT 1