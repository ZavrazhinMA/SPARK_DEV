object SparkProducer extends App with SparkSessionWrapper {

  val topic: String = "books"

  val csvToJson = spark.read
                   .format("csv")
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("src/main/scala/source/bestsellers.csv")
                   .withColumnRenamed("User Rating", "Rating")
                   .toJSON

  csvToJson
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", topic)
    .option("checkpointLocation", "src/main/scala/source")
    .save()

}
