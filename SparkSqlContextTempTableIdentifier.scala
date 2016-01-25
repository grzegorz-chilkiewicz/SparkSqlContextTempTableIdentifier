import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object SparkSqlContextTempTableIdentifier {

  private def identifierCheck(df: DataFrame, identifier: String): Unit = {
    df.registerTempTable(identifier)
    df.sqlContext.dropTempTable(identifier)
  }

  def main(args: Array[String]): Unit = {
    println("Initializing Spark")
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("SparkSqlContextTempTableIdentifier")
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val rows = List(Row("foo"), Row("bar"))
    val schema = StructType(Seq(StructField("col", StringType)))
    val rdd = sc.parallelize(rows)
    val df = sqlContext.createDataFrame(rdd, schema)

    val valid1 = "df"
    val valid2 = "674123a"
    val valid3 = "674123_"
    val valid4 = "a0e97c59_4445_479d_a7ef_d770e3874123"
    val valid5 = "1ae97c59_4445_479d_a7ef_d770e3874123"
    val fail   = "10e97c59_4445_479d_a7ef_d770e3874123" // This is invalid identifier!

    println("Valid identifiers")
    identifierCheck(df, valid1)
    identifierCheck(df, valid2)
    identifierCheck(df, valid3)
    identifierCheck(df, valid4)
    identifierCheck(df, valid5)

    println("Invalid identifier")
    try {
      identifierCheck(df, fail)
    } catch {
      case e: RuntimeException =>
        println("Exception caught!")
        e.printStackTrace()
    }
  }
}
