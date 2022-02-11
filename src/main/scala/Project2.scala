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
  }

  


  def queryOne(spark: SparkSession): Unit = {

    // Load in covid-data.csv
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects TOTAL CASES
    df.select("location","total_cases").groupBy("location").agg(max("total_cases")).distinct().show()
  }

  def queryTwo(spark: SparkSession): Unit = {

    // Load in covid-data.csv
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects TOTAL DISTINCT CASES in 'Asia' 
    df.select("location","total_cases").where(col("continent") === "Asia").groupBy("location").agg(max("total_cases")).distinct().show()
   
  }

  def queryThree(spark: SparkSession): Unit = {
    // Load in covid-data.csv
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects MAX cases in 'Asia' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).where(col("continent") === "Asia").show()

  }

  def queryFour(spark: SparkSession): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects MAX cases in 'Africa' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).where(col("continent") === "Africa").show()
  }

  def queryFive(spark: SparkSession): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects MAX cases in 'Europe' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).where(col("continent") === "Europe").show()
  }

  def querySix(spark: SparkSession): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects MAX cases in 'North America' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).where(col("continent") === "North America").show()
  }

  def querySeven(spark: SparkSession): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects MAX cases in 'South America' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).where(col("continent") === "South America").show()
  }

  def queryEight(spark: SparkSession): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load("input/covid-data.csv")

    // Selects MAX cases in 'Oceania' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).where(col("continent") === "Oceania").show()
  }

  def queryNine(): Unit = {
    
  }

  def queryTen(): Unit = {
    
  }
}
