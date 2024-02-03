## Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--rm`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15612
```
select
	count(*)
from green_taxi_data
where 
	lpep_pickup_datetime::date = '2019-09-18'::date
	and lpep_dropoff_datetime::date = '2019-09-18'::date
;
```

## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-26

```
select
	lpep_pickup_datetime::date,
	trip_distance
from green_taxi_data
where 
	lpep_pickup_datetime::date in ( '2019-09-18'::date, '2019-09-16'::date, '2019-09-26'::date, '2019-09-21'::date)
order by 2 desc	
;
```

## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- "Brooklyn" "Manhattan" "Queens"

```
select
	"Borough",
	sum(total_amount)
from green_taxi_data taxi
left join 
	taxi_zone_lookup zone_
on
	taxi."PULocationID" = zone_."LocationID"
where 
	lpep_pickup_datetime::date = '2019-09-18'::date
	and zone_."Borough" != 'Unknown'
group by 1
having sum(total_amount) > 50000
;
```


## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- JFK Airport

```
select
	do_zone."Zone",
	tip_amount
from green_taxi_data taxi
left join 
	taxi_zone_lookup pu_zone
on
	taxi."PULocationID" = pu_zone."LocationID"
left join 
	taxi_zone_lookup do_zone
on
	taxi."DOLocationID" = do_zone."LocationID"
where 
	date_trunc('month',lpep_pickup_datetime::date) = '2019-09-01'::date
	and pu_zone."Zone" = 'Astoria'
ORDER BY tip_amount desc
;
```


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET