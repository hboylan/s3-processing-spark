# Spark Yelp Data

Spark JAR to load yelp data into Redshift


## Introduction

Spark SQL is able to store structured data in-memory for efficient processing. Our use case is to load source data from S3 to a Spark DataFrame, perform some processing, and load the DataFrame into an OLAP database.


## Build

- `sbt assembly`
- FAT JAR with all dependencies written to `target/scala-2.11/unseenstars-spark-assembly-1.0.jar`
- Upload to hadoop master node `scp -i /path/to/unseenstars.pem /path/to/target/scala-2.11/unseenstars-spark-assembly-1.0.jar hadoop@MASTER_NODE_DNS:~`
- SSH to hadoop master node `ssh -i /path/to/unseenstars.pem hadoop@MASTER_NODE_DNS`


## Run

#### Redshift
```bash
spark-submit \
--class YelpData \
--conf spark.custom.db.type=redshift \
--conf spark.custom.db.url=jdbc:redshift://unseenstars.abc123.us-east-2.redshift.amazonaws.com:5439 \
--conf spark.custom.db.user=unseenstars \
--conf spark.custom.db.password=somepassword123 \
--conf spark.custom.db.name=dev \
--conf spark.custom.db.schema=public \
unseenstars-spark-assembly-1.0.jar
```

#### Snowflake
```bash
spark-submit \
--class YelpData \
--conf spark.custom.db.type=snowflake \
--conf spark.custom.db.url=abc123.us-east-1.snowflakecomputing.com \
--conf spark.custom.db.user=unseenstars \
--conf spark.custom.db.password=somepassword123 \
--conf spark.custom.db.name=demo_db \
--conf spark.custom.db.schema=public \
--conf spark.custom.db.warehouse=unseenstars \
unseenstars-spark-assembly-1.0.jar
```


## Configuration

<table>
 <tr>
    <th>Key</th>
    <th>Required</th>
    <th>Default</th>
    <th>Notes</th>
 </tr>

 <tr>
    <td><tt>spark.custom.db.type</tt></td>
    <td>No</td>
    <td><tt>redshift</tt></td>
    <td>The output database type (<tt>redshift</tt> or <tt>snowflake</tt>)</td>
 </tr>

 <tr>
    <td><tt>spark.custom.db.url</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>URL for database connection</td>
 </tr>

 <tr>
    <td><tt>spark.custom.db.user</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>Database user</td>
 </tr>

 <tr>
    <td><tt>spark.custom.db.password</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>Database password</td>
 </tr>

 <tr>
    <td><tt>spark.custom.db.name</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>Database name</td>
 </tr>

 <tr>
    <td><tt>spark.custom.db.schema</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>Database schema</td>
 </tr>

 <tr>
    <td><tt>spark.custom.db.warehouse</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>Snowflake warehouse to process COPY command</td>
 </tr>
</table>


## Metrics

<table>
 <tr>
    <th>Table</th>
    <th>Rows</th>
    <th>Size Redshift</th>
    <th>Size Snowflake</th>
    <th>Time Redshift</th>
    <th>Time Snowflake</th>
 </tr>

 <tr>
    <td><tt>yelp_user</tt></td>
    <td>1.5M</td>
    <td>80MB</td>
    <td><b>42.8MB</b></td>
    <td><b>98s</b></td>
    <td>107s</td>
 </tr>

 <tr>
    <td><tt>yelp_business</tt></td>
    <td>188.6K</td>
    <td>252MB</td>
    <td><b>18MB</b></td>
    <td>23s</td>
    <td><b>20s</b></td>
 </tr>

 <tr>
    <td><tt>yelp_business_tip</tt></td>
    <td>1.2M</td>
    <td>124MB</td>
    <td><b>63.9MB</b></td>
    <td><b>30s</b></td>
    <td>31s</td>
 </tr>

 <tr>
    <td><tt>yelp_business_checkin</tt></td>
    <td>4.2M</td>
    <td>80MB</td>
    <td><b>18.9MB</b></td>
    <td><b>99s</b></td>
    <td>108s</td>
 </tr>

 <tr>
    <td><tt>yelp_business_review</tt></td>
    <td>5.8M</td>
    <td>2.3GB</td>
    <td><b>1.9GB</b></td>
    <td>412s</td>
    <td><b>339s</b></td>
 </tr>

 <tr>
    <td><tt>yelp_business_review_words</tt></td>
    <td>257.6M</td>
    <td>4.1GB</td>
    <td><b>1.1GB</b></td>
    <td><b>736s</b></td>
    <td>831s</td>
 </tr>

 <tr>
    <td><tt>yelp_business_review_sentiment</tt></td>
    <td>1.8M</td>
    <td>184MB</td>
    <td><b>136.6MB</b></td>
    <td>85s</td>
    <td><b>75s</b></td>
 </tr>
</table>
