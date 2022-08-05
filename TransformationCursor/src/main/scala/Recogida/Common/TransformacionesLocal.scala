package Recogida.Common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{coalesce, col, lit, to_timestamp}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.util.Date

object TransformacionesLocal extends Common {

  var dataframes=Map(
    "DWE_SGE_SAP_PROVEEDORES"->spark.read.option("header","true").csv("./DWE_SGE_SAP_PROVEEDORES_20220624_165906.csv.bz2"),
    "DWE_SGR_MU_ASIG_OPERADORES_UF"->spark.read.option("header","true").csv("./DWE_SGR_MU_ASIG_OPERADORES_UF_TMP_20220624_165920.csv.bz2"),
    "DWE_SGR_MU_ASIG_OPERADORES_UTE"->spark.read.option("header","true").csv("./DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP_20220624_165929.csv.bz2"),
    //DWE_VM_COMUAUTO
    "C"->spark.read.option("header","true").csv("./DWE_VM_COMUAUTO_20220624_171044.csv.bz2"),
    //DWE_VM_ELTREMED NO SE USA
    "ELTREMED"->spark.read.option("header","true").csv("./DWE_VM_ELTREMED_20220624_172354.csv.bz2"),
    //DWE_VM_ELTREPOB // en algun lado está como P2
    "E"->spark.read.option("header","true").csv("./DWE_VM_ELTREPOB_20220624_172746.csv.bz2"),
    //DWE_VM_ENTLOCAL
    "L"->spark.read.option("header","true").csv("./DWE_VM_ENTLOCAL_20220624_171535.csv.bz2"),
    //DWE_VM_ENTLTPRE
    "R"->spark.read.option("header","true").csv("./DWE_VM_ENTLTPRE_20220624_172757.csv.bz2"),
    //DWE_VM_POBPERST
    "P"->spark.read.option("header","true").csv("./DWE_VM_POBPERST_20220624_172950.csv.bz2"),
    //DWE_VM_TIPOLENT
    "S"->spark.read.option("header","true").csv("./DWE_VM_TIPOLENT_20220624_174014.csv.bz2"),
    //DWE_VM_TIPOLFAC está como TP
    "TPL"->spark.read.option("header","true").csv("./DWE_VM_TIPOLFAC_20220624_174344.csv.bz2"),
    //DWE_VM_TPRECOGI
    "TP"->spark.read.option("header","true").csv("./DWE_VM_TPRECOGI_20220624_174341.csv.bz2"),
    //DWE_VM_UAACTIVI
    "U"->spark.read.option("header","true").csv("./DWE_VM_UAACTIVI_20220624_174209.csv.bz2"),
    //DWE_VM_UFTRGMUN
    "T"->spark.read.option("header","true").csv("./DWE_VM_UFTRGMUN_20220624_174447.csv.bz2"),
    //DWE_VM_UFUGACTI TAMBIÉN ESTÁ COMO UF Y UF2
    "F"->spark.read.option("header","true").csv("./DWE_VM_UFUGACTI_20220624_174339.csv.bz2"),
    //DWE_VM_UGACTIVI
    "G"->spark.read.option("header","true").csv("./DWE_VM_UGACTIVI_20220624_174307.csv.bz2"),
    //DWE_VM_UGACTMUN
    "M"->spark.read.option("header","true").csv("./DWE_VM_UGACTMUN_20220624_174425.csv.bz2"),
    //DWE_VM_UNIDADMI
    "UA"->spark.read.option("header","true").csv("./DWE_VM_UNIDADMI_20220624_174438.csv.bz2"),
    //DWE_VM_VOLUMENS NO SE USA
    "VOL"->spark.read.option("header","true").csv("./DWE_VM_VOLUMENS_20220714_125755.csv.bz2"),
  )

  def RecogidaInicial(date: String): Unit = {
    def OPtable():DataFrame={
      dataframes("DWE_SGR_MU_ASIG_OPERADORES_UTE").alias("OU").join(dataframes("DWE_SGR_MU_ASIG_OPERADORES_UF").alias("OP"),col("OU.UTE_ID")===col("OP.UTE_ID"),"left")
      .selectExpr("(1 * coalesce(OP.PORCENTAJE_QT, 100) / 100) * coalesce(OU.PORCENTAJE_QT, 100) / 100 AS POBDC_QT"," OP.OPERADOR_ID as OPERADOR_ID_OP","OU.OPERADOR_ID as OPERADOR_ID_OU","COALESCE(OP.OPERADOR_ID, OU.OPERADOR_ID, 0) AS OPERADOR_ID"," CASE WHEN OP.OPERADOR_ID IS NOT NULL THEN OP.PORCENTAJE_QT ELSE OU.PORCENTAJE_QT END as PORCENTAJE_QT","coalesce(OP.UTE_ID,0) AS UTE_ID"," CASE WHEN OP.UTE_ID IS NOT NULL THEN OP.PORCENTAJE_QT ELSE NULL END AS PORCENTAJE_UTE_QT","OP.DESDE_DT","OP.HASTA_DT","OP.UFUGA_ID","OP.MEDIOSPP_SN").as("OP").join(dataframes("F").as("UF2"),col("UF2.UFUGA_ID")===col("OP.UFUGA_ID"),"right")

    }
    OPtable().show(10)
}



  def CargaMedios(): Unit = {
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilos


}
