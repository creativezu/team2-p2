import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project1Ex {
  def main(args: Array[String]): Unit = {

    // This block of code is all necessary for spark/hive/hadoop
    System.setSecurityManager(null)
    /*System.setProperty(
      "hadoop.home.dir",
      "C:\\hadoop\\"
    ) */ // change if winutils.exe is in a different bin folder
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Project1Ex") // Change to whatever app name you want
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val hiveCtx = new HiveContext(sc)
    import hiveCtx.implicits._

    //This block to connect to mySQL
    val driver = "com.mysql.cj.jdbc.Driver"
    val url =
      "jdbc:mysql://localhost:3306/p1" // Modify for whatever port you are running your DB on
    val username = "root"
    val password = "70Sevenkawkroot!" // Update to include your password
    var connection: Connection = null

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    // Method to check login credentials
    val adminCheck = login(connection)
    if (adminCheck) {
      println("Welcome Admin! Loading in data...")
    } else {
      println("Welcome User! Loading in data...")
    }

    // Run method to insert Covid data. Only needs to be ran initially, then table data1 will be persisted.
    insertCovidData(hiveCtx)

    /*
     * Here is where I would ask the user for input on what queries they would like to run, as well as
     * method calls to run those queries. An example is below, top10DeathRates(hiveCtx)
     *
     */

    top10DeathRates(hiveCtx)

    sc.stop() // Necessary to close cleanly. Otherwise, spark will continue to run and run into problems.
  }

  // This method checks to see if a user-inputted username/password combo is part of a mySQL table.
  // Returns true if admin, false if basic user, gets stuckl in a loop until correct combo is inputted (FIX)
  def login(connection: Connection): Boolean = {

    while (true) {
      val statement = connection.createStatement()
      val statement2 = connection.createStatement()
      println("Enter username: ")
      var scanner = new Scanner(System.in)
      var username = scanner.nextLine().trim()

      println("Enter password: ")
      var password = scanner.nextLine().trim()
      val resultSet = statement.executeQuery(
        "SELECT COUNT(*) FROM admin_accounts WHERE username='" + username + "' AND password='" + password + "';"
      )
      while (resultSet.next()) {
        if (resultSet.getString(1) == "1") {
          return true;
        }
      }

      val resultSet2 = statement2.executeQuery(
        "SELECT COUNT(*) FROM user_accounts WHERE username='" + username + "' AND password='" + password + "';"
      )
      while (resultSet2.next()) {
        if (resultSet2.getString(1) == "1") {
          return false;
        }
      }

      println("Username/password combo not found. Try again!")
    }
    return false
  }

  def insertCovidData(hiveCtx: HiveContext): Unit = {
    //hiveCtx.sql("LOAD DATA LOCAL INPATH 'input/covid_19_data.txt' OVERWRITE INTO TABLE data1")
    //hiveCtx.sql("INSERT INTO data1 VALUES (1, 'date', 'California', 'US', 'update', 10, 1, 0)")

    // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
    // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can
    // then be
    val output = hiveCtx.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/covid_19_data.csv")
    output.limit(15).show() // Prints out the first 15 lines of the dataframe

    // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries.

    // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
    // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this
    // code as well as the creation of output will not be necessary.
    output.createOrReplaceTempView("temp_data")
    hiveCtx.sql(
      "CREATE TABLE IF NOT EXISTS data1 (SNo INT, ObservationDate STRING, Province_State STRING, Country_Region STRING, LastUpdate STRING, Confirmed INT, Deaths INT, Recovered INT)"
    )
    hiveCtx.sql("INSERT INTO data1 SELECT * FROM temp_data")

    // To query the data1 table. When we make a query, the result set ius stored using a dataframe. In order to print to the console,
    // we can use the .show() method.
    val summary = hiveCtx.sql("SELECT * FROM data1 LIMIT 10")
    summary.show()
  }

  def top10DeathRates(hiveCtx: HiveContext): Unit = {
    val result = hiveCtx.sql(
      "SELECT Province_State State, MAX(deaths)/MAX(confirmed) Death_Rate FROM data1 WHERE country_region='US' AND Province_State NOT LIKE '%,%' GROUP BY State ORDER BY Death_Rate DESC LIMIT 10"
    )
    result.show()
    result.write.csv("results/top10DeathRates")
  }
}
