0. Подготовка

https://www.docker.com/products/docker-desktop/
https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=header


git clone https://github.com/big-data-europe/docker-hive.git
cd docker-hive

docker-compose up -d

docker ps
# see NAMES
docker cp "..\22. HiveQL\employee.txt" docker-hive-hive-server-1:/opt
docker cp "..\22. HiveQL\employee_id.txt" docker-hive-hive-server-1:/opt
docker cp "..\22. HiveQL\employee_hr.txt" docker-hive-hive-server-1:/opt
docker cp "..\22. HiveQL\employee_contract.txt" docker-hive-hive-server-1:/opt


docker-compose exec hive-server bash

jdbc:hive2://localhost:10000

dfs -put -f /opt/employee.txt /opt/employee_contract.txt /opt/employee_hr.txt /opt/employee_id.txt /user/hive;



1. Create tables

DROP database hivetest cascade;
create database hivetest;

use hivetest;

drop table employee;
CREATE TABLE employee (
      name string,
      work_place ARRAY<string>,
      gender_age STRUCT<gender:string,age:int>,
      skills_score MAP<string,int>,
      depart_title MAP<STRING,ARRAY<STRING>>
) --stored as orc
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

select * from employee;

desc formatted employee;

-- load data from HDFS
LOAD DATA INPATH '/user/hive/employee.txt' OVERWRITE INTO TABLE hivetest.employee;

-- load data from local FS
LOAD DATA LOCAL INPATH '/opt/employee.txt' OVERWRITE INTO TABLE hivetest.employee;

CREATE TABLE IF NOT EXISTS employee_hr
(
    name string,
    employee_id int,
    sin_number string,
    start_date date
) -- stored as orc
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE;

desc formatted employee_hr;
    
LOAD DATA LOCAL INPATH '/opt/employee_hr.txt' OVERWRITE INTO TABLE employee_hr;

desc formatted hivetest.employee;

set hive.cli.print.current.db=true; -- ДЛЯ НОРМАЛЬНОГО ОТОБРАЖЕНИЯ ТАБЛИЦ В КОМАНДНОЙ СТРОКЕ
set hive.cli.print.header=true;

CREATE TABLE employee_id
(
    name string,
    employee_id int,
    work_place ARRAY<string>,
    gender_age STRUCT<gender:string,age:int>,
    skills_score MAP<string,int>,
    depart_title MAP<STRING,ARRAY<STRING>>
)
    ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '/opt/employee_id.txt' OVERWRITE INTO TABLE employee_id;

CREATE TABLE IF NOT EXISTS employee_contract
(
    name string,
    dept_num int,
    employee_id int,
    salary int,
    type string,
    start_date date
)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/employee_contract.txt' OVERWRITE INTO TABLE employee_contract;

2. Complex type

--Query the ARRAY in the table
SELECT work_place FROM hivetest.employee;


SELECT work_place[0] AS col_1,
       work_place[1] AS col_2,
       work_place[2] AS col_3
FROM employee;

--Query the STRUCT in the table
SELECT gender_age FROM employee;

SELECT gender_age.gender, gender_age.age FROM employee;

--Query the MAP in the table
SELECT skills_score FROM employee;

SELECT name, skills_score['DB'] AS DB,
       skills_score['Perl'] AS Perl, skills_score['Python'] AS Python,
       skills_score['Sales'] as Sales, skills_score['HR'] as HR FROM employee;

SELECT name,
       depart_title,
       depart_title['Product'][0] AS product_col0,
       depart_title['Test'][0] AS test_col0
FROM employee;


--Dynamic partition is not enabled by default. We need to set following to make it work.

SET hive.exec.dynamic.partition=true; -- ДЛЯ НОРМАЛЬНой работы динамического партиционирования
SET hive.exec.dynamic.partition.mode=nostrict;

CREATE TABLE employee_partitioned
(
    name string,
    work_place ARRAY<string>,
    gender_age STRUCT<gender:string,age:int>,
    skills_score MAP<string,int>,
    depart_title MAP<STRING,ARRAY<STRING>>
)
    PARTITIONED BY (Year INT, Month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

INSERT INTO TABLE employee_partitioned PARTITION(year, month)
SELECT name, array('Toronto') as work_place,
       named_struct("gender","Male","age",30) as gender_age,
       map("Python",90) as skills_score,
       map("R&D",array('Developer')) as depart_title,
    year(start_date) as year, month(start_date) as month
FROM employee_hr eh
WHERE eh.employee_id = 102;

INSERT INTO TABLE employee_partitioned PARTITION(year=2023, month=12)
SELECT name, array('Toronto') as work_place,
       named_struct("gender","Male","age",30) as gender_age,
       map("Python",90) as skills_score,
       map("R&D",array('Developer')) as depart_title
FROM employee_hr eh
WHERE eh.employee_id = 102


desc employee

CREATE TABLE employee_flat AS
SELECT
    name,
    place,
    gender_age.gender,
    gender_age.age
    FROM employee
             LATERAL VIEW explode(work_place) adTable AS place; --explode(ARRAY)

select * from employee_flat;

desc formatted employee_flat;

SELECT k, v FROM employee
    LATERAL VIEW explode(depart_title) myTable1 AS k,v; --explode(MAP)


--SELECT myCol1, myCol2 FROM baseTable
--    LATERAL VIEW explode(col1) myTable1 AS myCol1
--    LATERAL VIEW explode(col2) myTable2 AS myCol2;

3. Databases

--Create database with location, comments, and metadata information
CREATE DATABASE IF NOT EXISTS otus
COMMENT 'hive database demo'
LOCATION '/hdfs/directory'
WITH DBPROPERTIES ('creator'='Alexey','date'='2022-12-05');

--Show and describe database with wildcards
SHOW DATABASES;
SHOW DATABASES LIKE 'hive.*';
DESCRIBE DATABASE hivetest;

--Show current database
SELECT current_database();

4. Table creation

--Create internal table and load the data
CREATE TABLE IF NOT EXISTS hivetest.employee_internal (
     name string,
     work_place ARRAY<string>,
     gender_age STRUCT<gender:string,age:int>,
     skills_score MAP<string,int>,
     depart_title MAP<STRING,ARRAY<STRING>>
)
    COMMENT 'This is an internal table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':'
    STORED AS TEXTFILE;
    
LOAD DATA LOCAL INPATH '/opt/employee.txt' OVERWRITE INTO TABLE hivetest.employee_internal;

select * from employee_internal;

dfs -mkdir /user/hive/warehouse/employee_data;
dfs -put /opt/employee.txt /user/hive;

dfs -cp /user/hive/employee.txt /user/hive/warehouse/employee_data;

--Create external table and load the data
CREATE EXTERNAL TABLE IF NOT EXISTS employee_external (
    name string,
    work_place ARRAY<string>,
    gender_age STRUCT<gender:string,age:int>,
    skills_score MAP<string,int>,
    depart_title MAP<STRING,ARRAY<STRING>>
)
COMMENT 'This is an external table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/employee_data';

show create table employee_external;

msck repair table employee_external; -- если таблица партиционирована

desc formatted employee_external;

LOAD DATA LOCAL INPATH '/opt/employee.txt' OVERWRITE INTO TABLE employee_external;
select * from employee_external;

--Temporary tables (live only in session)
--In most cases you can use a normal table with DROP in the end of the script
--Or just use WITH operator
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 (
    name string,
    work_place ARRAY<string>,
    gender_age STRUCT<gender:string,age:int>,
    skills_score MAP<string,int>,
    depart_title MAP<STRING,ARRAY<STRING>>
);

insert into tmp_emp1 select * from employee_external;
select * from tmp_emp1;

--Create Table With Data - CREATE TABLE AS SELECT (CTAS)
CREATE TABLE ctas_employee AS SELECT * FROM employee_external;

CREATE TABLE cte_employee AS
WITH r1 AS (SELECT name FROM r2 WHERE name = 'Michael'),
     r2 AS (SELECT name FROM employee WHERE gender_age.gender= 'Male'),
     r3 AS (SELECT name FROM employee WHERE gender_age.gender= 'Female')
SELECT * FROM r1 UNION ALL select * FROM r3;


SELECT * FROM cte_employee;

--Create Table Without Data - TWO ways
--With CTAS
CREATE TABLE empty_ctas_employee AS SELECT * FROM employee_internal WHERE 1=2;

--With LIKE
CREATE TABLE empty_like_employee LIKE employee_internal;

--Check row count for both tables
SELECT COUNT(*) AS row_cnt FROM empty_ctas_employee;
SELECT COUNT(*) AS row_cnt FROM empty_like_employee;

5. Table description

--Show tables
SHOW TABLES;
   
SHOW TABLES '*emp*';
SHOW TABLES '*ext*|*cte*';
SHOW TABLE EXTENDED LIKE 'employee_int*';

--Show columns
SHOW COLUMNS IN employee;

DESC formatted employee;

--Show DDL and property
SHOW CREATE TABLE employee;
SHOW TBLPROPERTIES employee;

6. Table ALTER
   --Alter table name

--Alter table delimiter through SerDe properties
ALTER TABLE employee SET SERDEPROPERTIES ('field.delim' = '$');
select * from employee; --bad
ALTER TABLE employee SET SERDEPROPERTIES ('field.delim' = '|');
select * from employee; --OK

--Alter Table File Format (other formats https://cwiki.apache.org/confluence/display/Hive/FileFormats)
ALTER TABLE employee SET FILEFORMAT RCFILE;
select * from employee e; --error
ALTER TABLE employee SET FILEFORMAT TEXTFILE;
select * from employee e; --OK
--Alter Table employee Concatenate to merge small files into larger files
--convert to the file format supported (can't switch back from ORC) 
--ALTER TABLE employee SET FILEFORMAT ORC;


select * from employee e;
--Alter Table Location
ALTER TABLE employee SET LOCATION '/user/hive/employee';

--Alter columns
--Change column type - before changes
DESC employee;

--Change column type
ALTER TABLE employee CHANGE name employee_name string AFTER gender_age; --error
ALTER TABLE employee CHANGE name employee_name string;

--Verify the changes
DESC employee;

--Change column type
ALTER TABLE employee CHANGE employee_name name string COMMENT 'updated' FIRST;

--Verify the changes
DESC employee;

--Add columns to the table
ALTER TABLE employee ADD COLUMNS (work string);

--Verify the added columns
DESC employee;

--Replace all columns
--ALTER TABLE employee REPLACE COLUMNS (name string);

7. SELECT

SET hive.support.quoted.identifiers = none; -- чтоб работали регулярки

select * from employee e;

SELECT `^work.*` FROM employee;

SELECT `(gender_age|work_contractor)?+.+` FROM employee;

--https://cwiki.apache.org/confluence/display/hive/languagemanual+udf
--Select with UDF, IF, and CASE WHEN
SELECT
    CASE WHEN gender_age.gender = 'Female' THEN 'Ms.'
         ELSE 'Mr.' END as title,
    name,
    IF(array_contains(work_place, 'New York'), 'US', 'CA') as country
FROM employee;

select COALESCE(work_place[2], work_place[1], work_place[0]) FROM employee;

select name from employee
except select name from employee_contract;

select name from employee
intersect select name from employee_contract;

8. JOIN

SELECT *
FROM employee a
LEFT SEMI JOIN employee_id b ON a.name = b.name;--where in

9. INSERT EXPORT IMPORT

DROP TABLE IF EXISTS ctas_employee ;
CREATE TABLE ctas_employee AS SELECT * FROM employee;

set hive.support.quoted.identifiers=none;

select * from ctas_employee

create table employee_1 as SELECT `(gender_age)?+.+` from employee limit 0;
create table employee_2 as SELECT `(skills_score)?+.+` from employee limit 0;

--multitable INSERT from CTE
WITH a as (SELECT * FROM ctas_employee )
FROM a --join b
INSERT OVERWRITE TABLE employee_1
SELECT `(gender_age)?+.+` where a.work_place[0] = 'Montreal'
INSERT OVERWRITE TABLE employee_2
SELECT `(skills_score)?+.+` where a.work_place[0] <> 'Montreal'

select * from employee_1;
select * from employee_2;


--Export data and metadata of table
EXPORT TABLE employee TO '/tmp/output';

dfs -ls -R /tmp/output/;

--Import as new table
IMPORT TABLE employee_imported FROM '/tmp/output';

desc formatted employee_imported;

10. Basic aggregation

--Aggregate functions are used with COALESCE and IF
SELECT
    sum(coalesce(gender_age.age,0)) AS age_sum,
    sum(if(gender_age.gender = 'Female',1,0)),
    sum(if(gender_age.gender = 'Male',1,0))
FROM employee;

--Aggregate functions can be also used with DISTINCT keyword to do aggregation on unique values.
SELECT count(distinct gender_age.gender) AS gender_uni_cnt, count(distinct name) AS name_uni_cnt FROM employee;

--Use max/min struct
SELECT gender_age.gender,
       max(struct(gender_age.age, name)).col1 as age,
       max(struct(gender_age.age, name)).col2 as name
FROM employee
GROUP BY gender_age.gender;

11. Window functions

--window aggregate functions
SELECT
    name,
    dept_num as deptno,
    salary,
    count(*) OVER (PARTITION BY dept_num) as cnt,
    count(*) OVER (PARTITION BY dept_num order by name) as cnt2,
    sum(salary) OVER(PARTITION BY dept_num ORDER BY dept_num) as sum1,
    sum(salary) OVER(ORDER BY dept_num) as sum2
FROM employee_contract
ORDER BY deptno, name;


--window sorting functions
SELECT
    name,
    dept_num as deptno,
    salary,
    row_number() OVER (PARTITION BY dept_num ORDER BY salary) as rnum,
    rank() OVER (PARTITION BY dept_num ORDER BY salary) as rk,
    dense_rank() OVER (PARTITION BY dept_num ORDER BY salary) as drk,
    percent_rank() OVER(PARTITION BY dept_num ORDER BY salary) as prk,
    ntile(4) OVER(PARTITION BY dept_num ORDER BY salary) as ntile
FROM employee_contract
ORDER BY deptno, salary;

--window analytics function
SELECT
    name,
    dept_num,
    salary,
    round(cume_dist() OVER (PARTITION BY dept_num ORDER BY salary), 2) as cume,
    lead(salary) OVER (PARTITION BY dept_num ORDER BY salary) as lead,
    lag(salary, 2, 0) OVER (PARTITION BY dept_num ORDER BY salary) as lag,
    first_value(name) OVER (PARTITION BY dept_num ORDER BY salary) as fval,
    last_value(name) OVER (PARTITION BY dept_num ORDER BY salary) as lvalue,
    last_value(name) OVER (PARTITION BY dept_num ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lvalue2
FROM employee_contract
ORDER BY dept_num, salary;

--window expression preceding and following
SELECT
    name, dept_num as dno, salary AS sal,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) win1,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) win2,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) win3,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) win4,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) win5,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS 2 PRECEDING) win6,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS UNBOUNDED PRECEDING) win7
FROM employee_contract
ORDER BY dno, name;

--window expression current_row
SELECT
    name, dept_num as dno, salary AS sal,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND CURRENT ROW) win8,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) win9,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) win10,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) win11,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) win12,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) win13,
    max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) win14
FROM employee_contract
ORDER BY dno, name;

--window reference
SELECT name, dept_num, salary,
       MAX(salary) OVER w1 AS win1,
       MAX(salary) OVER w2 AS win2,
       MAX(salary) OVER w3 AS win3
FROM employee_contract
         WINDOW w1 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
                w2 as w3,
                w3 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
;


12. Hivevars

set hivevar:schema=hivetest;
set hivevar:min_age=30;

select * from ${schema}.employee_flat where age > ${min_age};

hive -e 'select * from ${schema}.employee_flat where age > ${min_age};' --hivevar schema=hivetest --hivevar min_age=30;
hive -f 1.sql --hivevar schema=hivetest --hivevar min_age=30;

13. MACROs

CREATE TEMPORARY MACRO is_leap_year(y int)
    case when y % 400 = 0 then TRUE
        when y % 100 = 0 then FALSE
        when y % 4 = 0 then TRUE
        else FALSE
    end;


select distinct name, age
from employee_flat
where is_leap_year(cast(from_unixtime(unix_timestamp(), "yyyy") as int) - age)


14. Limitations

SELECT * FROM employee_hr
WHERE name IN (SELECT name FROM employee WHERE name LIKE 'M%')
--OR --forbidden top level conjucts only 
AND
name IN (SELECT name FROM employee_contract WHERE name LIKE 'M%');

--Для джойна по дизъюнкции нужно включить настройки:

set hive.strict.checks.cartesian.product=false;
set hive.mapred.mode=nonstrict;

select * from employee_hr hr
join employee_contract c on hr.name = c.name or hr.sin_number=c.salary;

15. Тесты

select assert_true(count(*)=0)
from (
    select ... from sut
) x;

16. Clean up

docker-compose down
docker system prune -a


