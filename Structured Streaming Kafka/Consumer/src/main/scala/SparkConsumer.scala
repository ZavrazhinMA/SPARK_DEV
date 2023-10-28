import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructType}

object SparkConsumer extends App with SparkSessionWrapper {

  import spark.implicits._

  val topic: String = "books"

  val schema = ArrayType(new StructType()
      .add("Name", StringType)
      .add("Author", StringType)
      .add("Rating", DoubleType)
      .add("Reviews", LongType)
      .add("Price", IntegerType)
      .add("Year", IntegerType)
      .add("Genre", StringType))

  val jsonStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topic)
    .load()
    .selectExpr("CAST(value as STRING)")
    .withColumn("jsonData", explode(from_json(col("value"), schema)))
    .select("jsonData.*")
    .filter($"Rating" >= 4)

  jsonStream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", "src/main/result/chkp")
    .option("path", "src/main/result")
    .queryName("Bestsellers > 4")
    .start()
    .awaitTermination()
}
