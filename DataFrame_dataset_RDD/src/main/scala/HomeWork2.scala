import org.apache.spark.rdd.RDD

object HomeWork2 extends App with SparkSessionWrapper {

  /*
  Задание 2:
Загрузить данные в RDD из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
С помощью lambda построить таблицу, которая покажет в какое время происходит больше всего вызовов. Результат вывести на экран и в txt файл c пробелами.
Результат:
В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл.
Решение оформить в github gist.
   */

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

  val data_time = spark.read
    .format("parquet")
    .load("src/main/res/yellow_taxi_jan_25_2018").as[TaxiInfo]
    .rdd

  val result: RDD[String] = data_time
    .map(x => x.tpep_pickup_datetime.substring(11, 13))
    .groupBy(x => x)
    .map(s => Tuple2(s._1 , s._2.size))
    .sortBy(_._2, ascending=false)
    .map(x => s"${x._1} ${x._2.toString}")
    .cache()

  result.saveAsTextFile("src/main/res/result")
  result.foreach(println)

}
