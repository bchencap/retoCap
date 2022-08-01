package Recogida.Common

import org.apache.spark.sql.SparkSession

class Common
{
  val spark = SparkSession
    .builder()
    .appName("df1")
    .master("local")
    .getOrCreate()



}
