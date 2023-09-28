import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.count

object TestRun extends App{
  val spark = SparkSession.builder()
    .appName(name = "Test App")
    .getOrCreate()
  val data =spark.read.textFile("my_new_test.txt")

  import spark.implicits._

  data
    .flatMap(_.split(" "))
    .groupBy($"value")
    .agg(count($"value")).as("total")
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet("C:\\SPARK_DEV\\spark_part_1\\result")
  spark.read.parquet("C:\\SPARK_DEV\\spark_part_1\\result").show(false)
}
