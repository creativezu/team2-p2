import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
    .master("local[*]")
    .appName("Team 2 Project 2")
    .getOrCreate()

    // This block of code is all necessary for spark/hive/hadoop
    System.setSecurityManager(null)
    /*System.setProperty(
      "hadoop.home.dir",
      "C:\\hadoop\\"
    ) */ // change if winutils.exe is in a different bin folder
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Project2") // Change to whatever app name you want
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val hiveCtx = new HiveContext(sc)
    import hiveCtx.implicits._

    

    // Run method to insert Covid data. Only needs to be ran initially, then table data1A will be persisted.
    //insertCovidData(hiveCtx)

    /*
     * Here is where I would ask the user for input on what queries they would like to run, as well as
     * method calls to run those queries. An example is below, top10DeathRates(hiveCtx)
     *
     */

    top10DeathRates()
    maxDeaths()
    mostVaxxed()
    leastVaxxed()
    leastDeaths()
    oldestPop()
    youngestPop()


    sc.stop() // Necessary to close spark cleanly.
  }

  

  def insertCovidData(spark: SparkSession): Unit = {
    val df = spark.read.csv("input/covid-data.csv")
    val rddFromFile = spark.sparkContext.textFile("input/covid-data.csv") // Read CSV file
    val rdd = rddFromFile.map(f=>{f.split(",")}) // Skip header

    // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
    // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can
    // then be
    // val output = spark.read
    //   .format("csv")
    //   .option("inferSchema", "true")
    //   .option("header", "true")
    //   .load("input/covid-data.csv")
    // output.limit(15).show() // Prints out the first 15 lines of the dataframe
  }

  def top10DeathRates(): Unit = {
    
  }

  def maxDeaths(): Unit = {
    
  }

  def mostVaxxed(): Unit = {
    
  }

  def leastVaxxed(): Unit = {
    
  }

  def leastDeaths(): Unit = {
    
  }

  def oldestPop(): Unit = {
    
  }

  def youngestPop(): Unit = {
    
  }
}
