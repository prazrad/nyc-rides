# NYC Cab Ride POC
POC on monthly analytics on **yellow cab** and **for-hire** vehicle rides regarding the
following aspects:
1. Total number, duration, and distance of rides
2. Number of rides to and from the three airports of NYC (JFK, Newark,
LaGuard)

This is an simulated experiement to find the analytics result and should not be part of actual real world analytics senario. Here, I have used July(07) and August(08) - 2019 data for the analytical operation due to infrastructure environment.

Over all flow of the POC (High-level architecture):

![High-level Architecture](https://i.imgur.com/39yh6Uh.png)

After cloning this repo performing the following :

##### Note : Update the CSV file directory in the code level

### Start Spark Jobs

To get the result of 1st report - All ride information run "all_rides_report.py" using :
```./spark-submit all_rides_report.py```

To get the result of 2nd report - All airport ride information run "airport_rides_report.py" using :
```./spark-submit airport_rides_report.py```

This will generate partitioned "Parquet files" under ./output directory

####  Load the parquet files to Hive table using the following Hive Query (HQL) :
Create gerald schema :
```
CREATE SCHEMA gerald;
```

Create all_rides table :
```
CREATE EXTERNAL TABLE gerald.all_rides(
`month` int,
`total_rides` int,
`toal_duration` int,
`total_distance` int,
)
PARTITIONED BY (`cab` string) STORED AS PARQUET
LOCATION 'hdfs:///gerald/output/all_rides/'
```

Create airport_rides table :
```
CREATE EXTERNAL TABLE gerald.airport_rides(
`month` int,
`PULocationID` int,
`pickup_rides` int,
`DOLocationID` int,
`drop_rides` int
)
PARTITIONED BY (`cab` string) STORED AS PARQUET
LOCATION 'hdfs:///gerald/output/airport_rides/'
```

#### Data Visualization using Hue :

![Hue data visualization](https://i.imgur.com/mN5wu5D.png)


