import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object YelpData {

  val sc = initSparkContext("Yelp Users")
  val spark = initSparkSession()
  val yelpDataPartition = "s3://unseenstars/yelp_data/" // move to args
  val jdbcConnectionString = "REDACTED" // move to args

  def main(args: Array[String]): Unit = {

    //    read config json
    //    this.config = getJson(args(0))

    importUsers()
    importUserTips()
    importBusinesses()
    importReviews()
    importReviewWords()
    importReviewSentiment()
    importCheckins()

    // end
    sc.stop()
    spark.stop()
  }

  def initSparkContext(name: String): SparkContext = {
    val conf = new SparkConf()
      .setMaster("yarn")
      .setAppName(name)
    new SparkContext(conf)
  }

  def initSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .getOrCreate()
  }

  def getJson(path: String): DataFrame = {
    this.spark
      .read
      .json(path)
  }

  def getCsv(path: String, withHeader: Boolean = false): DataFrame = {
    var df = this.spark.read
    if (withHeader) {
      df = df.option("header", true)
    }
    df.csv(path)
  }

  def writeTable(dataFrame: DataFrame, tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    dataFrame
      .write
      .format("com.databricks.spark.redshift")
      .option("url", this.jdbcConnectionString)
      .option("dbtable", tableName)
      .option("tempdir", "s3://unseenstars/spark/temp")
      .option("forward_spark_s3_credentials", true)
      .option("tempformat", "CSV GZIP")
      .mode(mode)
      .save()
  }

  def importUsers(): Unit = {
    val users = getJson(s"${yelpDataPartition}yelp_user/yelp_academic_dataset_user.json")
      .withColumnRenamed("name", "user_name")
      .select($"user_id", $"user_name", $"average_stars", $"review_count", $"yelping_since")
    writeTable(users, "public.yelp_user")
  }

  def importUserTips(): Unit = {
    val tipMaxLength = 1024
    val tipMetadata = new MetadataBuilder()
      .putLong("maxlength", tipMaxLength)
      .build()
    val tips = getJson(s"${yelpDataPartition}/yelp_tip/yelp_academic_dataset_tip.json")
      .withColumnRenamed("text", "tip_text")
      .withColumnRenamed("date", "tip_date")
      .withColumnRenamed("likes", "likes_count")
      .where(length($"tip_text") <= tipMaxLength)
      .withColumn("tip_text", $"tip_text".as("tip_text", tipMetadata))
      .select("business_id", "user_id", "tip_text", "tip_date", "likes_count")
    writeTable(tips, "public.yelp_business_tip")
  }

  def importBusinesses(): Unit = {
    val businessCategoriesMetadata = new MetadataBuilder().putLong("maxlength", 600).build()
    val businesses = getJson(s"${yelpDataPartition}/yelp_business/yelp_business.csv")
      .withColumnRenamed("name", "business_name")
      .withColumnRenamed("address", "business_address")
      .withColumnRenamed("city", "business_city")
      .withColumnRenamed("state", "business_state")
      .withColumnRenamed("postal_code", "business_postal_code")
      .withColumnRenamed("latitude", "business_latitude")
      .withColumnRenamed("longitude", "business_longitude")
      .withColumnRenamed("stars", "stars_rating")
      .withColumnRenamed("is_open", "is_business_open")
      .withColumn("categories", $"categories".as("categories", businessCategoriesMetadata))
    writeTable(businesses, "public.yelp_business")
  }

  def importReviews(): Unit = {
    val reviewMaxLength = 2048
    val reviewMetadata = new MetadataBuilder()
      .putLong("maxlength", reviewMaxLength)
      .build()
    val reviews = getJson(s"${yelpDataPartition}/yelp_review/yelp_reviews_clean.json")
      .withColumnRenamed("date", "review_date")
      .withColumnRenamed("text", "review_text")
      .withColumnRenamed("stars", "star_rating")
      .where(length($"review_text") <= reviewMaxLength)
      .withColumn("review_text", $"review_text".as("review_text", reviewMetadata))
      .select("business_id", "user_id", "review_id", "review_date", "review_text", "star_rating")
    writeTable(reviews, "public.yelp_business_review")
  }

  def importReviewWords(): Unit = {
    val reviewWords = getCsv(s"${yelpDataPartition}/yelp_review/yelp_review_words.csv")
      .withColumnRenamed("_c0", "review_id")
      .withColumnRenamed("_c1", "word")
    writeTable(reviewWords, "public.yelp_business_review_words")
  }

  def importReviewSentiment(): Unit = {
    val reviewLines = getCsv(s"${yelpDataPartition}/yelp_review/comprehend/reviews.csv")
      .withColumnRenamed("_c0", "review_id")
      .withColumnRenamed("_c1", "line")
    val reviewLinesSentiment = getCsv(s"${yelpDataPartition}/yelp_review/comprehend_reviews.csv", true)
    val reviewSentiment = reviewLines
      .join(reviewLinesSentiment, "line")
      .drop("line")
    writeTable(reviewSentiment, "public.yelp_review_sentiment")
  }

  def importCheckins(): Unit = {
    val checkins = getJson(s"${yelpDataPartition}/yelp_checkin/yelp_checkins.json")
      .withColumnRenamed("week", "week_number")
      .withColumnRenamed("day", "day_name")
      .withColumnRenamed("checkins", "checkin_count")
      .select("business_id", "week_number", "day_name", "checkin_count")
    writeTable(checkins, "public.yelp_business_checkin")
  }
}
