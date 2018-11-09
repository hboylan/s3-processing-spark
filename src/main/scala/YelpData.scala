import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager

object YelpData {

  // configure logging
  val log = LogManager.getLogger("YelpData")

  // spark context and sql session
  val sc = initSparkContext("Yelp Data")
  val spark = initSparkSession()

  // configuration
  val dbType = sc.getConf.get("spark.custom.db.type", "redshift")
  val dbUrl = sc.getConf.get("spark.custom.db.url")
  val dbUser = sc.getConf.get("spark.custom.db.user")
  val dbPassword = sc.getConf.get("spark.custom.db.password")
  val dbName = sc.getConf.get("spark.custom.db.name")
  val dbSchema = sc.getConf.get("spark.custom.db.schema")
  val dbWarehouse = sc.getConf.get("spark.custom.db.warehouse", null) // snowflake only

  val s3Url = "s3://unseenstars/yelp_data/"

  def main(args: Array[String]): Unit = {

    // output configuration
    sc.getConf
        .getAll
        .foreach(log.info)

    // validate configuration
    validateConfig()

    // execute
    time { importUsers }
    time { importUserTips }
    time { importBusinesses }
    time { importBusinessCheckins }
    time { importReviews }
    time { importReviewWords }
    time { importReviewSentiment }

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

  def validateConfig(): Unit = {
    if (dbType == "snowflake" & dbWarehouse == null) {
      throw new Error(s"Missing spark.custom.db.warehouse! Required when spark.custom.db.type=snowflake")
    }
  }

  def getJson(path: String): DataFrame = {
    spark
      .read
      .json(path)
  }

  def getCsv(path: String, withHeader: Boolean = false): DataFrame = {
    var df = spark.read
    if (withHeader) {
      df = df.option("header", true)
    }
    df.csv(path)
  }

  def time[R](fn: => R): R = {
    val t0 = System.nanoTime()
    val result = fn
    val t1 = System.nanoTime()
    val duration = (t1 - t0) / 1e+9
    log.info(s"Elapsed time: ${duration} seconds")
    result
  }

  def writeTable(dataFrame: DataFrame, tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    if (dbType == "redshift") {
      writeTableRedshift(dataFrame, tableName, mode)
    }
    else if (dbType == "snowflake") {
      writeTableSnowflake(dataFrame, tableName, mode)
    }
    else {
      throw new Error(s"Invalid spark.custom.db.type specified: ${dbType}. Must be one of 'redshift' or 'snowflake'.")
    }
  }

  // https://github.com/databricks/spark-redshift#parameters
  def writeTableRedshift(dataFrame: DataFrame, tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    dataFrame
      .write
      .format("com.databricks.spark.redshift")
      .option("url", s"${dbUrl}/${dbName}")
      .option("user", dbUser)
      .option("password", dbPassword)
      .option("dbtable", s"${dbSchema}.${tableName}")
      .option("tempdir", "s3://unseenstars/spark/temp")
      .option("forward_spark_s3_credentials", true)
      .option("tempformat", "CSV GZIP")
      .mode(mode)
      .save()
  }

  // https://docs.snowflake.net/manuals/user-guide/spark-connector-use.html#setting-configuration-options-for-the-connector
  def writeTableSnowflake(dataFrame: DataFrame, tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    dataFrame
      .write
      .format("net.snowflake.spark.snowflake")
      .option("sfUrl", dbUrl)
      .option("sfUser", dbUser)
      .option("sfPassword", dbPassword)
      .option("sfDatabase", dbName)
      .option("sfSchema", dbSchema)
      .option("sfWarehouse", dbWarehouse)
      .option("dbtable", tableName)
      .mode(mode)
      .save()
  }

  def importUsers(): Unit = {
    val df = getJson(s"${s3Url}/yelp_user/yelp_academic_dataset_user.json")
      .withColumnRenamed("name", "user_name")
      .select("user_id", "user_name", "average_stars", "review_count", "yelping_since")
    writeTable(df, "yelp_user")
  }

  def importUserTips(): Unit = {
    import spark.implicits._
    val tipMaxLength = 1024
    val metadata = new MetadataBuilder()
      .putLong("maxlength", tipMaxLength)
      .build()
    val df = getJson(s"${s3Url}/yelp_tip/yelp_academic_dataset_tip.json")
      .withColumnRenamed("text", "tip_text")
      .withColumnRenamed("date", "tip_date")
      .withColumnRenamed("likes", "likes_count")
      .where(length($"tip_text") <= tipMaxLength)
      .withColumn("tip_text", $"tip_text".as("tip_text", metadata))
      .select("business_id", "user_id", "tip_text", "tip_date", "likes_count")
    writeTable(df, "yelp_business_tip")
  }

  def importBusinesses(): Unit = {
    import spark.implicits._
    val metadata = new MetadataBuilder().putLong("maxlength", 600).build()
    val df = getCsv(s"${s3Url}/yelp_business/yelp_business.csv", true)
      .withColumn("categories", $"categories".as("categories", metadata))
    writeTable(df, "yelp_business")
  }

  def importBusinessCheckins (): Unit = {
    val df = getJson(s"${s3Url}/yelp_checkin/yelp_checkins.json")
      .withColumnRenamed("week", "week_number")
      .withColumnRenamed("day", "day_name")
      .withColumnRenamed("checkins", "checkin_count")
      .select("business_id", "week_number", "day_name", "checkin_count")
    writeTable(df, "yelp_business_checkin")
  }

  def importReviews(): Unit = {
    import spark.implicits._
    val reviewMaxLength = 2048
    val metadata = new MetadataBuilder()
      .putLong("maxlength", reviewMaxLength)
      .build()
    val df = getJson(s"${s3Url}/yelp_review/yelp_reviews_clean.json")
      .withColumnRenamed("date", "review_date")
      .withColumnRenamed("text", "review_text")
      .withColumnRenamed("stars", "star_rating")
      .where(length($"review_text") <= reviewMaxLength)
      .withColumn("review_text", $"review_text".as("review_text", metadata))
      .select("business_id", "user_id", "review_id", "review_date", "review_text", "star_rating")
    writeTable(df, "yelp_business_review")
  }

  def importReviewWords(): Unit = {
    import spark.implicits._
    val df = getCsv(s"${s3Url}/yelp_review/yelp_review_words.csv")
      .withColumnRenamed("_c0", "review_id")
      .withColumnRenamed("_c1", "word")
      .where(length($"word") <= 15)
    writeTable(df, "yelp_business_review_words")
  }

  def importReviewSentiment(): Unit = {
    val dfReviews = getCsv(s"${s3Url}/yelp_review/comprehend/reviews.csv")
      .withColumnRenamed("_c0", "review_id")
      .withColumnRenamed("_c1", "line")
    val dfSentiment = getCsv(s"${s3Url}/yelp_review/comprehend_reviews.csv", true)
    val dfReviewSentiment = dfReviews
      .join(dfSentiment, "line")
      .drop("line")
    writeTable(dfReviewSentiment, "yelp_review_sentiment")
  }
}
