-- https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017
-- docker ps

-- docker cp ".\goalscorers.csv" docker-hive-hive-server-1:/opt
-- docker cp ".\shootouts.csv" docker-hive-hive-server-1:/opt
-- docker cp ".\results.csv" docker-hive-hive-server-1:/opt

-- docker-compose exec hive-server bash
--hdfs dfs -put -f /opt/goalscorers.csv /opt/shootouts.csv /opt/results.csv /user/hive

  DROP DATABASE IF EXISTS hiveql cascade;
CREATE DATABASE hiveql;

USE hiveql;

  DROP TABLE IF EXISTS goalscorers;
CREATE TABLE goalscorers (
	match_date DATE COMMENT "date of the match",
	 home_team STRING COMMENT "the name of the home team",
     away_team STRING COMMENT "the name of the away team",
          team STRING COMMENT "name of the team scoring the goal",
        scorer STRING COMMENT "name of the player scoring the goal",
   goal_minute INT COMMENT "minute of goal",
      own_goal BOOLEAN COMMENT "whether the goal was an own-goal",
       penalty BOOLEAN COMMENT "whether the goal was a penalty"
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");

  DROP TABLE IF EXISTS results;
CREATE TABLE results (
	match_date DATE COMMENT "date of the match",
     home_team STRING COMMENT "the name of the home team",
     away_team STRING COMMENT "the name of the away team",
    home_score INT COMMENT "full-time home team score including extra time, not including penalty-shootouts",
    away_score INT COMMENT "full-time away team score including extra time, not including penalty-shootouts",
    tournament STRING COMMENT "the name of the tournament",
          city STRING COMMENT "the name of the city/town/administrative unit where the match was played",
       country STRING COMMENT "the name of the country where the match was played",
       neutral BOOLEAN COMMENT "TRUE/FALSE column indicating whether the match was played at a neutral venue"
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");


  DROP TABLE IF EXISTS penalties;
CREATE TABLE penalties (
	match_date DATE COMMENT "date of the match",
     home_team STRING COMMENT "the name of the home team",
     away_team STRING COMMENT "the name of the away team",
        winner STRING COMMENT "winner of the penalty-shootout",
 first_shooter STRING COMMENT "the team that went first in the shootout"
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");


-- load data from HDFS
LOAD DATA INPATH '/user/hive/goalscorers.csv' OVERWRITE INTO TABLE hiveql.goalscorers;
LOAD DATA INPATH '/user/hive/results.csv' OVERWRITE INTO TABLE hiveql.results;
LOAD DATA INPATH '/user/hive/shootouts.csv' OVERWRITE INTO TABLE hiveql.penalties;

-- load data from local FS
--LOAD DATA LOCAL INPATH 'opt/goalscorers.csv' OVERWRITE INTO TABLE hiveql.goalscorers;
--LOAD DATA LOCAL INPATH'opt/results.csv' OVERWRITE INTO TABLE hiveql.results;
--LOAD DATA LOCAL INPATH'opt/shootouts.csv' OVERWRITE INTO TABLE hiveql.penalties;

SELECT * FROM penalties;
SELECT * FROM results;
SELECT * FROM goalscorers;

-- топ-10 авторов наибольшего количества автоголов
SELECT scorer, count(scorer) as autogoals
  FROM goalscorers
 WHERE own_goal = TRUE
 GROUP BY scorer
 ORDER BY autogoals DESC
 LIMIT 10;

-- самые быстрые голы на чемпионате мира
SELECT gs.match_date, gs.home_team, gs.away_team, gs.team, gs.scorer, gs.goal_minute, rs.tournament
  FROM goalscorers AS gs
  LEFT JOIN results AS rs
       ON gs.match_date = rs.match_date AND gs.home_team = rs.home_team AND gs.away_team = rs.away_team
 WHERE rs.tournament = "FIFA World Cup" AND gs.goal_minute = 1
 ORDER BY gs.match_date DESC;

-- в каких городах чаще всего исход матча решался по результатaм пенальти
SELECT rs.city, count(rs.city) AS penalty_win_number
  FROM penalties AS ps
  LEFT JOIN results AS rs
       ON ps.match_date = rs.match_date AND ps.home_team = rs.home_team AND ps.away_team = rs.away_team
 GROUP BY rs.city
 ORDER BY penalty_win_number DESC
 LIMIT 5;

-- лучшие бомбардиры чемпионатов мира
SELECT gs.scorer, count(gs.scorer) AS goals_number, rs.tournament
  FROM goalscorers AS gs
  LEFT JOIN results AS rs
       ON gs.match_date = rs.match_date AND gs.home_team = rs.home_team AND gs.away_team = rs.away_team
 WHERE rs.tournament = "FIFA World Cup" AND gs.own_goal <> TRUE
 GROUP BY rs.tournament, gs.scorer 
 ORDER BY goals_number DESC;

-- лучшие бомбардиры каждого чемпионата мира
DROP TABLE IF EXISTS tmp_emp1;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 AS SELECT * FROM
	(SELECT YEAR(gs.match_date) AS cup_year, gs.scorer AS scorer, count(gs.scorer) AS goals_number
       FROM goalscorers AS gs
       LEFT JOIN results AS rs
            ON gs.match_date = rs.match_date AND gs.home_team = rs.home_team AND gs.away_team = rs.away_team
      WHERE rs.tournament = "FIFA World Cup" AND gs.own_goal <> TRUE
      GROUP BY YEAR(gs.match_date), gs.scorer ) AS temrorary_table;

SELECT tm1.cup_year, tm1.scorer, tm2.max_goals FROM tmp_emp1 AS tm1
  LEFT JOIN
	  (SELECT cup_year, max(goals_number) AS max_goals
	   FROM tmp_emp1
	   GROUP BY cup_year) AS tm2
  	   ON tm2.cup_year = tm1.cup_year
 WHERE tm1.goals_number = tm2.max_goals
 ORDER BY tm1.cup_year DESC;

-- анализ количества забитых голов по странам в разрезе по годам на чемпионатах мира
SELECT DISTINCT(d.team),
	d.year_,
	sum(d.score) OVER w AS total_goals,
	ROUND((avg(d.score) OVER w), 1) AS avg_goals,
	ROUND((max(d.score) OVER w), 1) AS max_goals 
 FROM
(SELECT YEAR(match_date) AS year_, home_team AS team, home_score AS score
   FROM results
  WHERE tournament = "FIFA World Cup"
  UNION ALL
 SELECT YEAR(match_date) AS year_, away_team AS team, away_score AS score
   FROM results
  WHERE tournament = "FIFA World Cup") AS d
WINDOW w AS (PARTITION BY d.team, year_ ORDER BY d.year_ DESC)
 ORDER BY total_goals DESC;


--docker-compose down
--docker system prune -a

