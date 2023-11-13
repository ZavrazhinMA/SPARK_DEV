DROP schema IF EXISTS otus;
CREATE schema otus;

DROP TABLE IF EXISTS distance;
CREATE TABLE distance
(
    date_day VARCHAR(10),
    total_day_distance REAL,
    mean_day_distance REAL,
    max_day_distance REAL,
    min_day_distance REAL, 
    stddev_day_distance REAL
   
);