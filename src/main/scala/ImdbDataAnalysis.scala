import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ImdbDataAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("BGC Test").getOrCreate()

    // Read all input files
    // Notes: You will need to download the files to your local disk and update the paths below.
    // Alternative to point to the URL?  (eg: https://datasets.imdbws.com/name.basics.tsv.gz)
    val titleBasics = spark.read.option("sep", "\t").option("inferSchema", "true").option("header", "true").csv("/home/aby/IdeaProjects/SparkStuff/title.basics.tsv.gz")
    val titleCrew = spark.read.option("sep", "\t").option("inferSchema", "true").option("header", "true").csv("/home/aby/IdeaProjects/SparkStuff/title.crew.tsv.gz")
    val nameBasics = spark.read.option("sep", "\t").option("inferSchema", "true").option("header", "true").csv("/home/aby/IdeaProjects/SparkStuff/name.basics.tsv.gz")
    val ratings = spark.read.option("sep", "\t").option("inferSchema", "true").option("header", "true").csv("/home/aby/IdeaProjects/SparkStuff/title.ratings.tsv.gz")

    val filteredRatings = ratings.filter(col("numVotes") >= 50) // Only consider ratings with at least 50 votes
    val votesSum: Long = filteredRatings.agg(sum("numVotes").cast("long")).first.getLong(0)
    val votesCount: Long = filteredRatings.agg(count("numVotes").cast("long")).first.getLong(0)
    val avgNumOfVotes: Long = votesSum  / votesCount  // We probably don't need floating points here

    val calcNewRating = (numVotes: Int, avgVotes: Long, avgRating: Float) => math.rint((numVotes.toFloat / avgVotes) * avgRating * 10) / 10 // round to 1 decimal point
    val newCol = udf(calcNewRating).apply(col("numVotes"), lit(avgNumOfVotes), col("averageRating"))
    val newRatings = filteredRatings.withColumn("newRating", newCol)  // add the new column to existing DF
    val top20Ratings = newRatings.sort(col("newRating").desc).limit(20)

    // Question 1
    val top20Movies = top20Ratings.join(titleBasics, Seq("tconst"))

    // Question 2  - part 1
    val top20MovieNames = top20Movies.select("primaryTitle").collect().toList  // List the top 20 movie names
    println("Top 20 movie names, descending order: \n" + top20MovieNames.mkString("\n"))


    // To get the top credited persons, group by the nconst, and map to primaryName
    import spark.sqlContext.implicits._
    val crewOfTopMovies = top20Movies.select("tconst").join(titleCrew, Seq("tconst"))
    val directorsOfTopMovies = crewOfTopMovies.select("directors").map(_.getString(0)).collect().flatMap(_.split(",").toList)
    val writersOfTopMovies = crewOfTopMovies.select("writers").map(_.getString(0)).collect().flatMap(_.split(",").toList)
    val allCrewIds = directorsOfTopMovies ++ writersOfTopMovies
    // Get a frequency map of crews, and create it as a DataFrame
    val groupedCrewIds = allCrewIds.groupBy(n => n).map {case (k,v) => (k, v.length)}.toSeq.toDF("nconst", "numberOfCredits")
    val topCreditedCrew = groupedCrewIds.join(nameBasics, Seq("nconst")).sort(col("numberOfCredits").desc) // join with the names table to get real names from Ids

    // Question 2  - part 2
    val topCreditedCrewNames = topCreditedCrew.select("primaryName").collect().toList
    println("Top credited crew (descending order) across top 20 movies:\n" + topCreditedCrewNames.mkString("\n"))

    spark.stop()

  }
}

