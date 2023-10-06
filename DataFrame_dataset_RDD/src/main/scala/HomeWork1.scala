object HomeWork1 extends App with SparkSessionWrapper {
/*
Задание 1:
Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
Загрузить данные во второй DataFrame из файла со справочными данными поездок в csv (src/main/resources/data/taxi_zones.csv)
С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов. Результат вывести на экран и записать в файл Паркет.
Результат:
В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл.
Решение оформить в github gist.
 */
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, SaveMode}


  val taxiData: DataFrame = spark.read
    .format("parquet")
    .load("src/main/res/yellow_taxi_jan_25_2018")

  val locationInfo: DataFrame  = spark.read
    .format("csv")
    .option("mode", "FAILFAST")
    .option("header", "true")
    .load("src/main/res/taxi_zones.csv")

  taxiData
    .groupBy("PULocationID")
    .agg(count("*").as("orders_count"))
    .join(locationInfo, taxiData("PULocationID") === locationInfo("LocationID"), "left")
    .drop("PULocationID")
    .select("orders_count", "LocationID", "Borough", "Zone")
    .orderBy(col("orders_count").desc)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/res/result")

  spark.read.parquet("src/main/res/result").show(false)


}
