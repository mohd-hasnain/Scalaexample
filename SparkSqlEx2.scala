package com.spark.training

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object SparkSqlEx2 {

  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark SparkSqlEx1").setMaster("local[4]"))

//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = new org.apache.spark.sql.HiveContext(sc)

    val input = args(0)       // i/p file path C:\Users\Admin\Desktop\sparktraining\input\emp.txt
    
    val emp = sc.textFile(input)

    val schemaString = "id name age dept"

    val schema = StructType(
                  schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = emp.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3).trim))

    val empDf = sqlContext.createDataFrame(rowRDD, schema)
    
    empDf.registerTempTable("emp")

    val empres = sqlContext.sql("SELECT id, name, age FROM user WHERE dept = 10")

    empres.show()
    
    sc.stop()
  }
}