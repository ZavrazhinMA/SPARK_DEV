import org.apache.spark.sql.Dataset

import java.util.Properties

object HomeWork3 extends App with SparkSessionWrapper {
/*
Задание 3:
Загрузить данные в DataSet из файла с фактическими данными поездок в
Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
С помощью DSL и lambda построить таблицу, которая покажет.
Как происходит распределение поездок по дистанции.
Результат вывести на экран и записать в СУБД PostgreSQL (файл docker-compose в корневой папке проекта).
Для записи в базу данных необходимо продумать и также приложить init SQL файл со структурой.
(Пример: можно построить витрину со следующими колонками:
общее количество поездок, среднее расстояние, среднеквадратическое отклонение, минимальное и максимальное расстояние)
Результат:
В консоли должны появиться данные с результирующей таблицей, в СУБД должна появиться таблица.
 */
  import org.apache.spark.sql.functions._
  import spark.implicits._


  case class TaxiInfo(vendorID: Int,
                      tpep_pickup_datetime: String,
                      tpep_dropoff_datetime: String,
                      passenger_count: Int,
                      trip_distance: Double,
                      RatecodeID: Int,
                      store_and_fwd_flag: String,
                      PULocationID: Int,
                      DOLocationID: Int,
                      payment_type: Int,
                      fare_amount: Double,
                      extra: Double,
                      mta_tax: Double,
                      tip_amount: Double,
                      tolls_amount: Double,
                      improvement_surcharge: Double,
                      total_amount: Double,
                     )

  object TaxiInfo {
    val taxiDf: Dataset[TaxiInfo] = spark.read
      .format("parquet")
      .load("src/main/res/yellow_taxi_jan_25_2018").as[TaxiInfo]
  }

  val result = TaxiInfo.taxiDf
    .withColumn("date_day", date_format(col("tpep_pickup_datetime"), "dd-MM-yyyy"))
    .groupBy("date_day")
    .agg(
      round(sum(col("trip_distance")), 2).as("total_day_distance"),
      round(mean(col("trip_distance")), 2).as("mean_day_distance"),
      round(max(col("trip_distance")), 2).as("max_day_distance"),
      round(min(col("trip_distance")), 2).as("min_day_distance"),
      round(stddev(col("trip_distance")), 2).as("stddev_day_distance")
    )
    .cache()

  val connectionProperties = new Properties()
  connectionProperties.put("user", "docker")
  connectionProperties.put("password", "docker")

  result.show()

  result
    .write
    .mode("overwrite")
    .jdbc(
      url = "jdbc:postgresql://localhost:5432/otus",
      table = "distance",
      connectionProperties = connectionProperties
    )
}
