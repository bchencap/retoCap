package Recogida.Common

import Recogida.spark
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.storage.StorageLevel
import spark.implicits._

import java.sql.Timestamp
import java.util.Date

object TransformacionesLocal extends {

  def RecogidaInicial(date: String): Unit = {
  }



  def CargaMedios(): Unit = {
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilos


}
