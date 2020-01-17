package com.ibm.featureprocessing

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.mongodb.spark._
import com.mongodb.spark.config._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{current_timestamp, sum}

class ProcessingFunctions {

  def getMongoData(spark: SparkSession,database:String,collection:String,columns:Array[String]): DataFrame ={
    //Changes for local setup
    val uri: String = s"mongodb://mongouser:mongouser@mongodb/${database}.${collection}"
    //val uri: String = s"mongodb://mongouser:mongouser@127.0.0.1:34000/${database}.${collection}"

    val customReadConfig = ReadConfig(
      Map("uri" -> uri)
    )

    //val dfRead = spark.read.format("mongo").option("uri", uri).load()
    val dfRead = spark.read.format("mongo").options(customReadConfig.asOptions).load()

    val result = dfRead.select(columns.head, columns.tail: _*)
    result
  }

  def getMariaData(spark: SparkSession,database:String,tableName:String,columns:Array[String]): DataFrame ={
    //Changes for local setup
    val jdbcHostname = "mysql"
    val jdbcPort = 3306
    //val jdbcHostname = "127.0.0.1"
    //val jdbcPort = 34006

    val jdbcDatabase = database

    val jdbcUsername = "mysql"
    val jdbcPassword = "mysql"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")
    val dfRead = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    val result = dfRead.select(columns.head, columns.tail: _*)
    result
  }

  def getMongoDataFromSQL(spark: SparkSession,database:String,collection:String,sql:String): DataFrame ={
    //Changes for local setup
    val uri: String = s"mongodb://mongouser:mongouser@mongodb/${database}.${collection}"
    //val uri: String = s"mongodb://mongouser:mongouser@127.0.0.1:34000/${database}.${collection}"
    val customReadConfig = ReadConfig(
      Map("uri" -> uri)
    )

    //val dfRead = spark.read.format("mongo").option("uri", uri).load()
    val dfRead = spark.read.format("mongo").options(customReadConfig.asOptions).load()

    dfRead.createOrReplaceTempView(collection)
    val result = spark.sql(sql)
    result
  }

  def getSnowflakeDataFromSQL(spark: SparkSession,database:String,tableName:String,sql:String): DataFrame ={
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
    val dfRead = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    dfRead.createOrReplaceTempView(tableName)
    val result = spark.sql(sql)
    result
  }

  def getMariaDataFromSQL(spark: SparkSession,database:String,tableName:String,sql:String): DataFrame ={
    //local
    //val jdbcHostname = "127.0.0.1"
    //val jdbcPort = 34006
    val jdbcHostname = "mysql"
    val jdbcPort = 3306

    val jdbcDatabase = database
    val jdbcUsername = "mysql"
    val jdbcPassword = "mysql"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    val dfRead = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    dfRead.createOrReplaceTempView(tableName)
    val result = spark.sql(sql)
    result
  }

  def prepareAttrs(spark:SparkSession,inputJson:String): String ={
    import spark.implicits._
    var dfMaria= spark.emptyDataFrame
    var dfMongo= spark.emptyDataFrame
    var dfSnowflake= spark.emptyDataFrame
    val processingFunctions = new ProcessingFunctions()
    //val inputJson:String = "{'attributes':[{'desc':'Spend Amount','dbtype':'mongodb','table':'cards','column':'Spend_Amount','colT4mtn':'NA'},{'desc':'CustomerIdentifier','dbtype':'mongodb','table':'cards','column':'Customer_ID','colT4mtn':'NA'},{'desc':'CustomerIdentifier','dbtype':'mariadb','table':'demographics','column':'Customer_ID','colT4mtn':'NA'},{'desc':'BusinessDate','dbtype':'mongodb','table':'cards','column':'Business_Date','colT4mtn':'NA'},{'desc':'CardType','dbtype':'mongodb','table':'cards','column':'Card_Type','colT4mtn':'NA'},{'desc':'PaidAmount','dbtype':'mongodb','table':'cards','column':'Paid_Amount','colT4mtn':'NA'},{'desc':'DateofBirth','dbtype':'mariadb','table':'demographics','column':'DOB','colT4mtn':'NA'},{'desc':'MaritalStatus','dbtype':'mariadb','table':'demographics','column':'Marital_Status','colT4mtn':'NA'},{'desc':'PostalCode','dbtype':'mariadb','table':'demographics','column':'Postal_Code','colT4mtn':'NA'},{'desc':'SelfEmployedorNot','dbtype':'mariadb','table':'demographics','column':'Self_Employed','colT4mtn':'NA'}],'transformations':[{'DST4mtn':'RollingWindow','params':'(Spend_Amount,10,Customer_ID,Business_Date)','colName':'Calculated_Carry_Over_Amt'}],'datasetname':'sample'}"

    val jsonObject = new JSONObject(inputJson.trim())
    val keys = jsonObject.keys()

    var mongoDS = new JSONArray()
    var mongoTables = mutable.Set[String]()
    var mongoQueries = scala.collection.mutable.Map[String, String]()

    var mongoDatasets = new ArrayBuffer[Dataset[Row]]()
    var mariaDatasets = new ArrayBuffer[Dataset[Row]]()
    var snowflakeDatasets = new ArrayBuffer[Dataset[Row]]()

    var mariaDS = new JSONArray()
    var mariaTables = mutable.Set[String]()
    var mariaQueries = scala.collection.mutable.Map[String, String]()

    var snowflakeDS = new JSONArray()
    var snowflakeTables = mutable.Set[String]()
    var snowflakeQueries = scala.collection.mutable.Map[String, String]()

    println(inputJson)
    val attributesArray = jsonObject.getJSONArray("attributes")
    val len = attributesArray.length()
    for (i <- 1 to len) {
      val attrEntry = attributesArray.getJSONObject(i - 1)
      //println(attrEntry)
      if(attrEntry.getString("dbtype") == "mongodb"){
        mongoDS.put(attrEntry)
        mongoTables += attrEntry.getString("table")
      } else if(attrEntry.getString("dbtype") == "mariadb"){
        mariaDS.put(attrEntry)
        mariaTables += attrEntry.getString("table")
      } else if(attrEntry.getString("dbtype") == "snowflakedb"){
        snowflakeDS.put(attrEntry)
        snowflakeTables += attrEntry.getString("table")
      }
    }

    for(tableName <- mongoTables){
      var selectColumns = ""
      var firstEle:Boolean = true
      for (i <- 1 to mongoDS.length()) {
        val ele = mongoDS.getJSONObject(i - 1)
        if (ele.getString("table") == tableName) {
          if(!firstEle) {
            selectColumns += " , "
          }else{
            firstEle = false
          }
          if(ele.getString("colT4mtn") != "NA") {
            selectColumns += ele.getString("colT4mtn") + "(" + ele.getString("column") + ") as " + ele.getString("column")
          }else
          {
            selectColumns += ele.getString("column")
          }
        }
      }

      mongoQueries(tableName) = "select " + selectColumns + " from " + tableName
      println(mongoQueries)
    }

    for(tableName <- mariaTables){
      var selectColumns = ""
      println(tableName)
      var firstEle:Boolean = true
      for (i <- 1 to mariaDS.length()) {
        val ele = mariaDS.getJSONObject(i - 1)
        if (ele.getString("table") == tableName) {
          if(!firstEle) {
            selectColumns += " , "
          }else{
            firstEle = false
          }
          if(ele.getString("colT4mtn") != "NA") {
            selectColumns += ele.getString("colT4mtn") + "(" + ele.getString("column") + ") as " + ele.getString("column")
          }else
          {
            selectColumns += ele.getString("column")
          }
        }
      }

      mariaQueries(tableName) = "select " + selectColumns + " from " + tableName
      println(mariaQueries)
    }

    for(tableName <- snowflakeTables){
      var selectColumns = ""
      println(tableName)
      var firstEle:Boolean = true
      for (i <- 1 to snowflakeDS.length()) {
        val ele = snowflakeDS.getJSONObject(i - 1)
        if (ele.getString("table") == tableName) {
          if(!firstEle) {
            selectColumns += " , "
          }else{
            firstEle = false
          }
          if(ele.getString("colT4mtn") != "NA") {
            selectColumns += ele.getString("colT4mtn") + "(" + ele.getString("column") + ") as " + ele.getString("column")
          }else
          {
            selectColumns += ele.getString("column")
          }
        }
      }

      snowflakeQueries(tableName) = "select " + selectColumns + " from " + tableName
      println(snowflakeQueries)
    }


    for( (tableName,sql) <- mongoQueries){
      mongoDatasets += processingFunctions.getMongoDataFromSQL(spark,"sampledb",tableName,sql)
    }

    for( (tableName,sql) <- mariaQueries){
      mariaDatasets += processingFunctions.getMariaDataFromSQL(spark,"sampledb",tableName,sql)
    }

    for( (tableName,sql) <- snowflakeQueries){
      snowflakeDatasets += processingFunctions.getSnowflakeDataFromSQL(spark,"sampledb",tableName,sql)
    }

    if(mongoDatasets.length > 0){
      dfMongo = mongoDatasets(0)
      var i=1
      while(i <= (mongoDatasets.length - 1) ){
        dfMongo = dfMongo.join(mongoDatasets(i),"Customer_ID")
        i+1
      }
    }

    if(mariaDatasets.length > 0){
      dfMaria = mariaDatasets(0)
      var i=1
      while(i <= (mariaDatasets.length - 1) ){
        dfMaria = dfMaria.join(mariaDatasets(i),"Customer_ID")
        i+1
      }
    }

    if(snowflakeDatasets.length > 0){
      dfSnowflake = snowflakeDatasets(0)
      var i=1
      while(i <= (snowflakeDatasets.length - 1) ){
        dfSnowflake = dfSnowflake.join(snowflakeDatasets(i),"Customer_ID")
        i+1
      }
    }

    dfMongo.printSchema()
    dfMaria.printSchema()
    dfSnowflake.printSchema()

    var resulSet = spark.emptyDataFrame
    if(mongoDatasets.length == 0){
      if(mariaDatasets.length == 0){
        if(snowflakeDatasets.length == 0){
          resulSet = dfSnowflake
        } else {
          resulSet = dfSnowflake
        }
      } else {
        if(snowflakeDatasets.length == 0){
          resulSet = dfMaria
        } else {
          resulSet = dfSnowflake.join(dfMaria,"Customer_ID")
        }
      }
    } else if(mariaDatasets.length == 0) {
      if(snowflakeDatasets.length == 0){
        resulSet = dfMongo
      } else {
        resulSet = dfSnowflake.join(dfMongo,"Customer_ID")
      }
    } else if(snowflakeDatasets.length == 0) {
      resulSet = dfMaria
    } else {
      resulSet = dfSnowflake.join(dfMongo,"Customer_ID").join(dfMaria,"Customer_ID")
    }

    resulSet.show()
    var resulSetTemp = spark.emptyDataFrame
    import spark.implicits._
    val transformationArray = jsonObject.getJSONArray("transformations")
    val transLen = transformationArray.length()
    for (i <- 1 to transLen) {
      val transformEntry = transformationArray.getJSONObject(i - 1)
      val transFunc = transformEntry.getString("DST4mtn")
      val colName = transformEntry.getString("colName")
      val params = transformEntry.getString("params")
      println(params)
      println(transFunc)
      println(colName)
      val paramArray = params.replaceAll("[()]","").split(",")
      println(paramArray)
      if(transFunc == "RollingWindow"){
        val windowSpec = Window.partitionBy(paramArray(2)).orderBy(paramArray(3)).rowsBetween((paramArray(1).toInt * -1),0)
        resulSetTemp = resulSet.withColumn(colName,sum(resulSet(paramArray(0))).over(windowSpec))

        /* code for future when multiple functions are allowed
        val windowSpec = Window.partitionBy(paramArray(3)).orderBy(paramArray(4)).rowsBetween((paramArray(2).toInt * -1),0)
        if(paramArray(1)=="sum")
          resulSetTemp = resulSet.withColumn(colName,sum(resulSet(paramArray(1))).over(windowSpec))
        else if(paramArray(1)=="max")
          resulSetTemp = resulSet.withColumn(colName,max(resulSet(paramArray(1))).over(windowSpec))
        else if(paramArray(1)=="min")
          resulSetTemp = resulSet.withColumn(colName,min(resulSet(paramArray(1))).over(windowSpec))
        else if(paramArray(1)=="mean")
          resulSetTemp = resulSet.withColumn(colName,mean(resulSet(paramArray(1))).over(windowSpec))
         */
        resulSetTemp.show()
      }
    }
    resulSetTemp.printSchema()
    val resulSetFinal = resulSetTemp.withColumn("refresh_date",current_timestamp)
    println("final schema")
    resulSetFinal.printSchema()

    //change for local
    //resulSetFinal.write.option("uri", "mongodb://mongouser:mongouser@127.0.0.1:34000/sampledb").option("collection", "output_coll").format("mongo").mode(SaveMode.Append).save()
    resulSetFinal.write.option("uri", "mongodb://mongouser:mongouser@mongodb/sampledb").option("collection", "output_coll").format("mongo").mode(SaveMode.Append).save()

    return s"Resultset is uploaded in collection 'output_coll'"
  }

}
