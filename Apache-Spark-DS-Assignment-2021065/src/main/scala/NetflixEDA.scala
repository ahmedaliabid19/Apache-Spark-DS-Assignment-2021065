import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import java.nio.file.{Files, Paths, StandardOpenOption}

object NetflixAnalysis {
  def main(args: Array[String]): Unit = {
    // Reduce logging noise
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("Netflix Data Analysis")
      .master("local[*]")
      .getOrCreate()

    val dataPath = "./netflix_titles.csv"
    val netflixDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataPath)

    val outputDirectory = "./results"
    Files.createDirectories(Paths.get(outputDirectory))

    // Save schema to file
    exportToFile(s"$outputDirectory/schema_description.txt", netflixDF.schema.treeString)

    // Save a sample of the dataset
    exportDataFrame(netflixDF.limit(10), s"$outputDirectory/sample")

    // Count by content type
    val contentTypeCounts = netflixDF.groupBy("type").count()
    exportDataFrame(contentTypeCounts, s"$outputDirectory/content_type_counts")

    // Top 10 countries with most entries
    val mostFrequentCountries = netflixDF.groupBy("country")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    exportDataFrame(mostFrequentCountries, s"$outputDirectory/top_countries")

    // Popular genres
    val genreCounts = netflixDF.groupBy("listed_in")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    exportDataFrame(genreCounts, s"$outputDirectory/popular_genres")

    // Number of titles released per year
    val yearlyTitleCounts = netflixDF.filter(col("release_year").isNotNull)
      .groupBy("release_year")
      .count()
      .orderBy("release_year")
    exportDataFrame(yearlyTitleCounts, s"$outputDirectory/titles_by_year")

    // Null value counts per column
    val nullValueReport = netflixDF.columns.map { columnName =>
      val nullCount = netflixDF.filter(col(columnName).isNull || col(columnName) === "").count()
      s"$columnName: $nullCount"
    }
    exportToFile(s"$outputDirectory/null_value_report.txt", nullValueReport.mkString("\n"))

    spark.stop()
  }

  // Helper method to save DataFrame to file
  def exportDataFrame(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
  }

  // Helper method to write text content to a file
  def exportToFile(filePath: String, textContent: String): Unit = {
    Files.write(Paths.get(filePath), textContent.getBytes, StandardOpenOption.CREATE)
  }
}
