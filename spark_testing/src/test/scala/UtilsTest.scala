import DataFrameFunctions._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class UtilsTest extends AnyFlatSpec with SparkSessionTestWrapper
  with Logging with BeforeAndAfter with DataFrameComparer
{
  var testDF: DataFrame = _
  var countriesDF: DataFrame = _
  var languagesDF: DataFrame = _

  val necessaryColumns: Seq[String] = Seq("_id", "altSpellings", "area", "borders", "callingCode", "capital",
    "cca2","cca3", "ccn3", "cioc", "currency", "demonym", "landlocked", "languages", "latlng", "name", "region",
    "subregion", "tld", "translations")

  before {
    testDF = spark
      .read
      .format("json")
      .option("mode", "FAILFAST")
      .load("src/sources/countries.json")

    countriesDF = getCountriesInfo(testDF)
    languagesDF = getLanguagesInfo(testDF)
  }

  "This" should "print testDf schema and show" in {
    testDF.printSchema
    testDF.show
  }

  "testDF" should "contain all necessary columns" in {
    print(testDF)
    assert(necessaryColumns.forall(x => testDF.columns.contains(x)))
  }

  "getCountriesInfo" should "show countries with more then 5 neighbors" in {
    assert(countriesDF.select("NumBorders").filter(col("NumBorders") < 5).count() === 0)
  }

  "getLanguagesInfo" should "have top 3 languages the same as in etalon DF" in {
    import spark.implicits._
    val etalonTopLanguageDF = Seq("English", "French", "Arabic").toDF("Language")
    assertSmallDataFrameEquality(languagesDF.select(col("Language")).limit(3), etalonTopLanguageDF)
  }

  "getLanguagesInfo" should "have etalon struture schema" in {
    val arrayStructureSchema = new StructType()
      .add("Language",StringType)
      .add("Countries", ArrayType(StringType, containsNull = false), nullable = false)
      .add("NumCountries", LongType, nullable = false)
     assert(languagesDF.schema === arrayStructureSchema)
  }
}