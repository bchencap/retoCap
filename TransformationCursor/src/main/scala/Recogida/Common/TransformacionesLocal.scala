package Recogida.Common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
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
    print(dataframes.apply("C").take(10).apply(0).toString())
   val TF =  dataframes("TLP").filter((dataframes("TPL")("DESDE_DT").between("2017-07-01","2017-07-31")) || (dataframes("TPL")("DESDE_DT").lt(lit ("2017-07-01")) && (dataframes("TPL")("HASTA_DT").gt(lit("2017-07-01")) || (dataframes("TPL")("HASTA_DT").isNull)))).join(dataframes("R"),col("ELMUN_ID") === dataframes("R")("ELMUN_ID"), "right")
   val UFjoin =  dataframes("F").join(dataframes("U"),dataframes("F")("UGACT_ID") === dataframes("U")("UGACT_ID"), "left")
     .join(dataframes("E"),dataframes("F")("DESDE_DT") <=  dataframes("E")("HASTA_DT") && (dataframes("F")("HASTA_DT"), dataframes("E")("DESDE_DT").isNull >= dataframes("E")("DESDE_DT")))
     .join(dataframes("DWE_SGE_SAP_PROVEEDORES"),dataframes("F")("UNFAC_ID") === dataframes("DWE_SGE_SAP_PROVEEDORES")("PROVE_ID"), "right" )

    //
    //dataframes("F").join(dataframes("E"),dataframes("F")("DESDE_DT") <=  dataframes("E")("HASTA_DT") && (dataframes("F")("HASTA_DT"), dataframes("E")("DESDE_DT").isNull >= dataframes("E")("DESDE_DT")))
    // V2 = DWE_SGE_SAP_PROVEEDORES
    //dataframes("F").join(dataframes("DWE_SGE_SAP_PROVEEDORES"),dataframes("F")("UNFAC_ID") === dataframes("DWE_SGE_SAP_PROVEEDORES")("PROVE_ID"), "right" )


  }



  def CargaMedios(): Unit = {
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilos


}
