package Recogida.Common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class Common
{

  val OS: String = System.getProperty("os.name").toLowerCase
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  if (OS.contains("win")) System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")
  else System.setProperty("hadoop.home.dir", "/")
  val spark = SparkSession
    .builder()
    .appName("df1")
    .master("local")
    .getOrCreate()

}
