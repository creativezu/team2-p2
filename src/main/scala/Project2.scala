import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object Project2 {
  def main(args:Array[String]): Unit = {
        val spark : SparkSession = SparkSession
                    .builder
                    .appName("SparkVSCode")
                    .config("spark.master", "local")
                    .getOrCreate()

    

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Feel free to rename these functions
    queryOne(spark)
    queryTwo(spark)
    queryThree(spark)
    queryFour(spark)
    queryFive(spark)
    querySix(spark)
    querySeven(spark)
    queryEight(spark)
    queryNine()
    queryTen()


    spark.stop() // Necessary to close spark cleanly.
  def queryOne(spark: SparkSession): Unit = {
    // Selects TOTAL CASES
    df.select("location","total_cases").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def queryTwo(spark: SparkSession): Unit = {
    // Selects MAX cases in 'Asia' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).show()
  }

  def queryThree(spark: SparkSession): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Asia' 
    df.select("location","total_cases").where(col("continent") === "Asia").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def queryFour(spark: SparkSession): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Africa' 
    df.select("location","total_cases").where(col("continent") === "Africa").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def queryFive(spark: SparkSession): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Europe' 
    df.select("location","total_cases").where(col("continent") === "Europe").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def querySix(spark: SparkSession): Unit = {
    // Selects TOTAL DISTINCT CASES in 'North America' 
    df.select("location","total_cases").where(col("continent") === "North America").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def querySeven(spark: SparkSession): Unit = {
    // Selects TOTAL DISTINCT CASES in 'South America' 
    df.select("location","total_cases").where(col("continent") === "South America").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def queryEight(spark: SparkSession): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Oceania' 
    df.select("location","total_cases").where(col("continent") === "Oceania").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def queryNine(): Unit = {

  }

  def queryTen(): Unit = {

  }
}
}
