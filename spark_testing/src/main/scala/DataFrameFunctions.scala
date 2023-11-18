import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataFrameFunctions extends App with SparkSessionWrapper {
  import spark.implicits._

  val countriesDF = spark
    .read
    .format("json")
    .option("mode", "FAILFAST")
    .load("src/sources/countries.json")

  def getCountriesInfo(df: DataFrame): DataFrame = {

    val cca3toCountries = df
      .select("cca3", "name.official")
      .collect()
      .map(row => row(0) -> row(1)).toMap.asInstanceOf[Map[String, String]]

    val countriesToString = (cca3List: Seq[String]) => {
      cca3List.map(x => cca3toCountries(x)).mkString(sep=", ")
    }
    val udfFunc = udf(countriesToString)

    val resultDF = df
      .select(col("name.official").as("Country"), col("borders"))
      .withColumn("NumBorders", size(col("borders")))
      .filter(col("NumBorders") >= 5)
      .withColumn("BorderCountries", udfFunc($"borders"))
      .drop(col("borders"))
      .orderBy(col("NumBorders").desc)
      .toDF()

    resultDF
  }

  def getLanguagesInfo(df: DataFrame): DataFrame = {

    val resultDF = df
      .select(col("name.official"), explode(array($"languages.*")))
      .filter(col("col") =!= "NULL")
      .groupBy(col("col").as("Language"))
      .agg(collect_set($"official").as("Countries"), count($"official").as("NumCountries"))
      .orderBy(col("NumCountries").desc)
    resultDF
  }
  getCountriesInfo(countriesDF).show(5, truncate = false)
  getLanguagesInfo(countriesDF).show(10)
}
