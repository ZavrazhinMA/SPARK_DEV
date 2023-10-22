import org.apache.spark.sql.SaveMode

object SparkConsumer extends App with SparkSessionWrapper {

  import spark.implicits._

  val jsonStream = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "books")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .selectExpr("CAST(value as STRING)")
    .as[String]


  val resultDF: Unit = spark
    .read
    .json(jsonStream)
    .filter($"rating" >= 4.0)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/scala/source/parquet")

//  spark.read.parquet("src/main/scala/source/parquet").show(false)

}
