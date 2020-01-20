package com.ibm.featureprocessing

import java.util.Properties

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object ScalaMain extends App{
 /*
//SNOWFLAKE data loading
  val conf = new SparkConf()
  conf.setJars(Seq("C:\\data\\apps\\projects\\datamicroservices\\dataserviceapis\\src\\main\\resources\\snowflake-jdbc-3.11.0.jar"))
  // SparkSession and configurations
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("TestSnowflakeConnection")
    //.config("spark.ui.enabled", "false")
    //.config(conf)
    .getOrCreate()

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl = s"jdbc:snowflake://uy65137.us-central1.gcp.snowflakecomputing.com/"

  // Create a Properties() object to hold the parameters.
  import java.util.Properties
  val connectionProperties = new Properties()

  connectionProperties.put("user", "vikrambhosle")
  connectionProperties.put("password", "Vikyman7")
  //connectionProperties.put("account", "vikrambhosle");  // replace "" with your account name
  connectionProperties.put("db", "sampledb");       // replace "" with target database name
  connectionProperties.put("schema", "public");   // replace "" with target schema name
  connectionProperties.put("driver", "net.snowflake.client.jdbc.SnowflakeDriver")
  //connectionProperties.put("tracing", "on")
  // val dfRead = spark.read.jdbc(jdbcUrl, "CUSTOMER", connectionProperties)
  // dfRead.show()

  val df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\data\\apps\\projects\\datamicroservices\\openshift_spark\\src\\main\\resources\\demographics.csv")
  val df3=df1.withColumn("dbtype",lit("snowflake"))
  df3.write.mode("overwrite").jdbc(jdbcUrl, "demographics", connectionProperties)

  val df2 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\data\\apps\\projects\\datamicroservices\\openshift_spark\\src\\main\\resources\\cards.csv")
  val df4 = df2.withColumn("dbtype",(lit("snowflake")))
  df4.write.mode("overwrite").jdbc(jdbcUrl, "cards", connectionProperties)
*/

  //Mongodb and MySQL database loading
    val spark = SparkSession.builder
      .appName("MihuPraju")
      .master("local[*]")
      .getOrCreate()

    val pf:ProcessingFunctions = new ProcessingFunctions()
    pf.prepareAttrs(spark, "{'attributes':[{'desc':'Customer Identifier','dbtype':'snowflake','table':'cards','column':'Customer_ID','colT4mtn':'NA'},{'desc':'Business Date','dbtype':'snowflake','table':'cards','column':'Business_Date','colT4mtn':'NA'},{'desc':'Card Type','dbtype':'snowflake','table':'cards','column':'Card_Type','colT4mtn':'NA'},{'desc':'Spend Amount','dbtype':'snowflake','table':'cards','column':'Spend_Amount','colT4mtn':'NA'}],'transformations':[{'DST4mtn':'RollingWindow','params':'(Spend_Amount,5,Customer_Id,Business_Date)','colName':'Calculated_Carry_Over_Amt'}],'datasetname':'sample'}");

    //code for loading data in to tables
/*
    val jdbcHostname = "localhost"
    val jdbcPort = 34006
    val jdbcDatabase = "sampledb"
    val jdbcUsername = "mysql"
    val jdbcPassword = "mysql"

    val connectionProperties = new Properties()
    connectionProperties.put("user", jdbcUsername)
    connectionProperties.put("password", jdbcPassword)
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
    //val mongoUrl:String = s"mongodb://mongouser:mongouser@mongodb/sampledb"

    val mongoUrl:String = s"mongodb://mongouser:mongouser@127.0.0.1:34000/sampledb"
    val df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\data\\apps\\projects\\datamicroservices\\openshift_spark\\src\\main\\resources\\demographics.csv")
    df1.withColumn("dbtype",(lit("sql"))).write.mode("overwrite").jdbc(jdbcUrl, "demographics", connectionProperties)
    df1.withColumn("dbtype",(lit("mongo"))).write.option("uri", mongoUrl).option("collection", "demographics").format("mongo").mode("overwrite").save()

    val df2 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\data\\apps\\projects\\datamicroservices\\openshift_spark\\src\\main\\resources\\cards.csv")
    df2.withColumn("dbtype",(lit("sql"))).write.mode("overwrite").jdbc(jdbcUrl, "cards", connectionProperties)
    df2.withColumn("dbtype",(lit("mongo"))).write.option("uri", mongoUrl).option("collection", "cards").format("mongo").mode("overwrite").save()
    df2.withColumn("dbtype",(lit("mongo"))).write.option("uri", mongoUrl).option("collection", "output_coll").format("mongo").mode("overwrite").save()

     */
}

