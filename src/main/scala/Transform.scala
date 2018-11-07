import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}

object Transform {

  val sc = initSparkContext()
  val spark = initSparkSession()

  def main(args: Array[String]): Unit = {

    // read config json
    val config = getJson(args(0))
      .head()

    // read schema paths
    val inputSchemaPath = config
      .getAs("input_schema")
      .toString
    val outputSchemaPath = config
      .getAs("output_schema")
      .toString

    // read schema
//    val inputSchema = getSchema(inputSchemaPath)
//    val outputSchema = getSchema(outputSchemaPath)

//    import org.apache.spark.sql.SaveMode
//    val users = spark.read.json("s3://unseenstars/yelp_data/yelp_user/yelp_academic_dataset_user.json").persist()
//    users.createOrReplaceTempView("yelp_users")
//    val yelp_users = spark.sql("SELECT user_id, name, average_stars, review_count, yelping_since FROM yelp_users").toDF("user_id", "user_name", "average_stars", "review_count", "yelping_since")
//    yelp_users.write.format("com.databricks.spark.redshift").option("url", "jdbc:redshift://unseenstars-dw-instance.cf8sutql5uaf.us-east-2.redshift.amazonaws.com:5439/unseenstars?user=hboylan&password=Pinnacle4").option("dbtable", "public.yelp_users").option("tempdir", "s3://unseenstars/spark/temp").option("forward_spark_s3_credentials", true).option("tempformat", "CSV").mode(SaveMode.Overwrite).save()

    // end
    this.sc.stop()
    this.spark.stop()
  }

  def initSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Transformation")
    new SparkContext(conf)
  }

  def initSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .getOrCreate()
  }

  def getJson(path: String): DataFrame = {
    this.spark.read.json(path)
  }

//  def getSchema(path: String): StructType = {
//    val schemaJson = getJson(path)
//      .collect()
//      .toList
//      .mkString(" ")
//
//    this.spark
//      .read
//      .schema(schemaJson)
//      .load()
//  }
}
