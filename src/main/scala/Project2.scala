import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object Project2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("SparkVSCode")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", true)
      .load("input/covid-data.csv")

    // queryOne(spark, df)
    // queryTwo(spark, df)
    // queryThree(spark, df)
    // queryFour(spark, df)
    // queryFive(spark, df)
    // querySix(spark, df)
    // querySeven(spark, df)
    // queryEight(spark, df)
    // queryNine(spark, df)
    // queryTen(spark, df)
    // queryEleven(spark, df)
    // queryTwelve(spark, df)
    // queryThirteen(spark, df)

    spark.stop()

  }
  def queryOne(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL CASES
    df.select("location", "total_cases")
      .groupBy("location")
      .agg(max("total_cases"))
      .distinct()
      .na
      .drop("any")
      .show(false)
  }

  def queryTwo(spark: SparkSession, df: DataFrame): Unit = {
    // Selects MAX cases in 'Asia'
    df.select("continent", "location", "total_cases")
      .groupBy("continent")
      .agg(max("total_cases"))
      .na
      .drop("any")
      .show(false)
  }

  def queryThree(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Asia'
    df.select("location", "total_cases")
      .where(col("continent") === "Asia")
      .groupBy("location")
      .agg(max("total_cases"))
      .distinct()
      .na
      .drop("any")
      .show(false)
  }

  def queryFour(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Africa'
    df.select("location", "total_cases")
      .where(col("continent") === "Africa")
      .groupBy("location")
      .agg(max("total_cases"))
      .distinct()
      .na
      .drop("any")
      .show(false)
  }

  def queryFive(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Europe'
    df.select("location", "total_cases")
      .where(col("continent") === "Europe")
      .groupBy("location")
      .agg(max("total_cases"))
      .distinct()
      .na
      .drop("any")
      .show(false)
  }

  def querySix(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL DISTINCT CASES in 'North America'
    df.select("location", "total_cases")
      .where(col("continent") === "North America")
      .groupBy("location")
      .agg(max("total_cases"))
      .distinct()
      .na
      .drop("any")
      .show(false)
  }

  def querySeven(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL DISTINCT CASES in 'South America'
    df.select("location", "total_cases")
      .where(col("continent") === "South America")
      .groupBy("location")
      .agg(max("total_cases"))
      .distinct()
      .na
      .drop("any")
      .show(false)
  }

  def queryEight(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL DISTINCT CASES in 'Oceania'
    df.select("location", "total_cases")
      .where(col("continent") === "Oceania")
      .groupBy("location")
      .agg(max("total_cases"))
      .distinct()
      .na
      .drop("any")
      .show(false)
  }

  def queryNine(spark: SparkSession, df: DataFrame): Unit = {
    // Selects TOTAL DEATHS
    df.select("location", "total_deaths")
      .groupBy("location")
      .agg(max("total_deaths"))
      .distinct()
      .na
      .drop("any")
      .na
      .drop("any")
      .show(false)
  }

  def queryTen(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceTempView("df")
    spark.sql("SELECT * FROM df")

    println("Vaccination Rate compared to Death Rate:")
    spark
      .sql(
        "SELECT date, ROUND((people_fully_vaccinated/population)*100, 2)AS vaccination_rate, ROUND((total_deaths/total_cases)*100, 2) AS death_rate FROM df WHERE location = \"United States\" AND date LIKE(\"%/1/2021%\") ORDER BY vaccination_rate DESC LIMIT 10"
      )
      .show()
  }

  def queryEleven(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceTempView("df")

    println("U.S. Death Rate for 2021")
    val q11 =
      spark.sql(
        "SELECT location, date, population, total_cases, (total_cases/population)*100 AS percentage_of_population_infected FROM df WHERE location LIKE(\"%States%\") AND date LIKE(\"%2021%\") ORDER BY date"
      )

    q11.show()
    q11
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .save("output/queryEleven")
  }

  def queryTwelve(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceTempView("df")

    println("Percentage of global population infected")
    val q12 =
      spark.sql(
        "SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS percentage_of_population_infected FROM df WHERE date LIKE (\"%2/7/2022%\") GROUP BY location, population ORDER BY percentage_of_population_infected DESC"
      )

    q12.show()
    q12
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("output/queryTwelve")
  }

  def queryThirteen(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceTempView("df")
    println("Percentage of global population vaccinated")

    val q13 = spark.sql(
      "SELECT location, date, population, MAX(people_fully_vaccinated) AS people_fully_vaccinated, MAX((people_fully_vaccinated/population))*100 AS percentage_of_people_fully_vaccinated FROM df WHERE date LIKE(\"%2/7/2022%\") GROUP BY location, date, population ORDER BY percentage_of_people_fully_vaccinated DESC"
    )

    q13
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("output/queryThirteen")

    q13.show()

  }
}
