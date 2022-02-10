import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

object Project2 {
  def main(args:Array[String]): Unit = {
        val spark = SparkSession
                    .builder
                    .appName("SparkVSCode")
                    .config("spark.master", "local")
                    .getOrCreate()

    import spark.implicits._

    // Load in covid-data.csv
    val df = spark.read.format("com.databricks.spark.csv").load("input/covid-data.csv")
    
    // Output csv file
    df.show


    // Feel free to rename these functions
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


    spark.stop() // Necessary to close spark cleanly.
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
