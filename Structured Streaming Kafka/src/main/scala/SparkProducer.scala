object SparkProducer extends App with SparkSessionWrapper {

  import spark.implicits._

  case class Bestsellers(
                        name: Option[String],
                        author: Option[String],
                        rating: Option[Double],
                        reviews: Option[Long],
                        price: Option[Double],
                        year: Option[Int],
                        genre: Option[String]
                        )

  val csvToJson = spark.read
                   .format("csv")
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("src/main/scala/source/bestsellers.csv")
                   .withColumnRenamed("User Rating", "rating")
                   .as[Bestsellers]
                   .toJSON


  csvToJson
    .selectExpr("CAST(value as STRING)")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "books")
    .option("checkpointLocation", "src/main/scala/source")
    .save()

}
