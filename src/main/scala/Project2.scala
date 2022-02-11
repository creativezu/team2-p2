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
    queryFour()
    queryFive()
    querySix()
    querySeven()
    queryEight()
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

    // Selects MAX cases in 'Aisia' 
    df.select("continent","location","total_cases").groupBy("continent").agg(max("total_cases")).where(col("continent") === "Asia").show()

  }

  def queryFour(): Unit = {
    
  }

  def queryFive(): Unit = {
    
  }

  def querySix(): Unit = {
    
  }

  def querySeven(): Unit = {
    
  }

  def queryEight(): Unit = {
    
  }

  def queryNine(): Unit = {
    
  }

  def queryTen(): Unit = {
    
  }
}
