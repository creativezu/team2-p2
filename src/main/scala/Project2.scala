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

    // This code is necessary for windows-based spark/hive/hadoop environments
    System.setSecurityManager(null)
    /*
      System.setProperty(
      "hadoop.home.dir",
      "C:\\hadoop\\"
    ) 
    */ 

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Project2") // Change to whatever app name you want
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    
    /*
     * Here is where we might take
     * in an input from user
     *
     */



    queryOne()
    queryTwo()
    queryThree()
    queryFour()
    queryFive()
    querySix()
    querySeven()
    queryEight()
    queryNine()
    queryTen()


    sc.stop() // Necessary to close spark cleanly.
  }

  

  def insertCovidData(spark: SparkSession): Unit = {
    val df = spark.read.csv("input/covid-data.csv")
    val rddFromFile = spark.sparkContext.textFile("input/covid-data.csv") // Read CSV file
    val rdd = rddFromFile.map(f=>{f.split(",")}) // Skip header

    /*
     * This statement creates a DataFrameReader from your file that you wish to pass in. 
     * We can infer the schema and retrieve column names if the first row in your csv
     * file has the column names. If not wanted, remove those options.
     */ 
     

    /*
    val output = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/covid-data.csv")
    output.limit(15).show() // Prints out the first 15 lines of the dataframe
    */
  }

  def queryOne(): Unit = {
    
  }

  def queryTwo(): Unit = {
    
  }

  def queryThree(): Unit = {
    
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
