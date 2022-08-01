import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

package object Recogida {

  val spark = SparkSession
    .builder()
    .appName("df1")
    .master("local")
    .getOrCreate()
}
