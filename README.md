# NYC Rideshare Analysis

## Overview
This repository contains the coursework for the Big Data Processing course, focusing on analyzing the New York 'Uber/Lyft' rideshare data from January 1, 2023, to May 31, 2023. The dataset, pre-processed by the NYC Taxi and Limousine Commission (TLC), is used to apply Spark techniques for various analytical tasks.

## Dataset
The dataset is provided under the path `//data-repository-bkt/ECS765/rideshare_2023/` and includes:
- `rideshare_data.csv`
- `taxi_zone_lookup.csv`

### Schema

#### rideshare_data.csv
| Field               | Type      | Description                                                                                              |
|---------------------|-----------|----------------------------------------------------------------------------------------------------------|
| business            | string    | Uber and Lyft.                                                                                           |
| pickup_location     | string    | Taxi Zone where the journey commenced. Refer to 'taxi_zone_lookup.csv' for details.                     |
| dropoff_location    | string    | Taxi Zone where the journey concluded. Refer to 'taxi_zone_lookup.csv' for details.                     |
| trip_length         | string    | The total distance of the trip in miles.                                                                 |
| request_to_pickup   | string (s)| The time taken from the ride request to the passenger pickup.                                            |
| total_ride_time     | string (s)| The duration between the passenger pickup and dropoff.                                                  |
| on_scene_to_pickup  | string (s)| The time duration between the driver's arrival on the scene and the passenger pickup.                   |
| on_scene_to_dropoff | string (s)| Time from the driver's arrival on the scene to the passenger dropoff.                                   |
| time_of_day         | string    | Categorization of the time of day: morning (0600-1100), afternoon (1200-1600), evening (1700-1900), night (other times). |
| date                | string (s)| The date when the ride was requested, expressed in UNIX timestamp.                                       |
| passenger_fare      | string    | The total fare paid by the passenger in USD.                                                             |
| driver_total_pay    | string    | The complete payment received by the driver.                                                            |
| rideshare_profit    | string    | The difference between the passenger fare and the driver's total pay.                                   |
| hourly_rate         | string    | The calculated hourly rate based on 'on_scene_hours'.                                                   |
| dollars_per_mile    | string    | The driver's earnings per mile, calculated as total pay divided by trip length.                          |

#### taxi_zone_lookup.csv
| LocationID | Borough            | Zone                   | service_zone |
|------------|--------------------|------------------------|--------------|
| 1          | EWR                | Newark Airport         | EWR          |
| 2          | Queens             | Jamaica Bay            | Boro Zone    |
| 3          | Bronx              | Allerton/Pelham Gardens| Boro Zone    |
| 4          | Manhattan          | Alphabet City          | Yellow Zone  |
| 5          | Staten Island      | Arden Heights          | Boro Zone    |

## Assignment Tasks

### Task 1: Merging Datasets (15 points)
- Load and join the datasets based on location fields.
- Convert the date field from UNIX timestamp to `yyyy-MM-dd` format.
- Print the number of rows and schema of the new dataframe.

### Task 2: Aggregation of Data (20 points)
- Count trips per business per month.
- Calculate platform profits per business per month.
- Calculate driver earnings per business per month.
- Provide insights from the results.

### Task 3: Top-K Processing (25 points)
- Identify the top 5 popular pickup and dropoff boroughs each month.
- Identify the top 30 earning routes.

### Task 4: Average of Data (15 points)
- Calculate the average driver pay and trip length for different times of the day.
- Calculate average earnings per mile for different times of the day.

### Task 5: Finding Anomalies (15 points)
- Calculate average waiting time in January.
- Identify and analyze days with waiting time exceeding 300 seconds.

### Task 6: Filtering Data (15 points)
- Find trip counts within a specified range for different pickup boroughs and times of day.
- Calculate trips in the evening time and trips from Brooklyn to Staten Island.

### Task 7: Routes Analysis (15 points)
- Analyze the top 10 popular routes based on trip count.

### Optional Task 8: Graph Processing (20 points)
- Define vertex and edge schemas and construct dataframes.


## License
The dataset is distributed under the MIT license.
