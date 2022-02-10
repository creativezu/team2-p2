import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Project2 {
  def main(args:Array[String]): Unit = {
        val spark = SparkSession
                    .builder
                    .appName("SparkVSCode")
                    .config("spark.master", "local")
                    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val rdd = spark.sparkContext.parallelize(Array((1, "a"), (2, "b"), (3, "c")))

    val df = rdd.toDF("num", "str")

    rdd.collect.foreach(println)
    df.printSchema
    df.show
    df.select("*").where($"num" > 1).show

    spark.stop() // Necessary to close spark cleanly.


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
