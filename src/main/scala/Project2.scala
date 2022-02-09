import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project2 {
  def main(args: Array[String]): Unit = {

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

    //top10DeathRates(hiveCtx)

    sc.stop() // Necessary to close cleanly. Otherwise, spark will continue to run and run into problems.
  }

  

  def insertCovidData(hiveCtx: HiveContext): Unit = {
    //hiveCtx.sql("LOAD DATA LOCAL INPATH 'input/covid_19_data.txt' OVERWRITE INTO TABLE data1A")
    //hiveCtx.sql("INSERT INTO data1A VALUES (1, 'date', 'California', 'US', 'update', 10, 1, 0)")

    // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
    // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can
    // then be
    val output = hiveCtx.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/covid-data.csv")
    output.limit(15).show() // Prints out the first 15 lines of the dataframe

    // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries.

    // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
    // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this
    // code as well as the creation of output will not be necessary.
    // output.createOrReplaceTempView("temp_data")
    // hiveCtx.sql(
    //   "CREATE TABLE IF NOT EXISTS data1A (iso_code INT, continent STRING, location STRING, date STRING, total_cases INT, new_cases INT, total_deaths INT, new_deaths INT, total_tests INT, total_vaccinations INT, people_vaccinated INT, people_fully_vaccinated INT, population INT, population_density INT, median_age INT, aged_65_older INT, aged_70_older INT, gdp_per_capita INT, hospital_beds_per_thousand INT, life_expectancy INT)"
    // )
    // hiveCtx.sql("INSERT INTO data1A SELECT * FROM temp_data")

    // To query the data1A table. When we make a query, the result set ius stored using a dataframe. In order to print to the console,
    // we can use the .show() method.
    // val summary = hiveCtx.sql("SELECT * FROM data1A LIMIT 10")
    // summary.show()
  }

  def top10DeathRates(hiveCtx: HiveContext): Unit = {
    val result = hiveCtx.sql(
      "SELECT continent, MAX(total_cases)/MAX(total_deaths) Death_Rate FROM data1A WHERE continent='Asia' GROUP BY continent ORDER BY Death_Rate DESC LIMIT 10"
    )
    result.show()
    result.write.csv("results/top10DeathRates")
  }
}
