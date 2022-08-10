package Recogida.Common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{coalesce, col, lit, to_timestamp}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, coalesce ,to_timestamp}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.Map

import java.sql.Timestamp
import java.util.Date


object TransformacionesLocal extends Common {

  var dataframes=Map(
    "V2"->spark.read.option("header","true").csv("./DWE_SGE_SAP_PROVEEDORES_20220624_165906.csv.bz2"),
    "V"->spark.read.option("header","true").csv("./DWE_SGE_SAP_PROVEEDORES_20220624_165906.csv.bz2"),
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
    def from(): Unit ={
      def OPtable():DataFrame={
        dataframes("DWE_SGR_MU_ASIG_OPERADORES_UTE").alias("OU").join(dataframes("DWE_SGR_MU_ASIG_OPERADORES_UF").alias("OP"),col("OU.UTE_ID")===col("OP.UTE_ID"),"left")
          .selectExpr("(1 * coalesce(OP.PORCENTAJE_QT, 100) / 100) * coalesce(OU.PORCENTAJE_QT, 100) / 100 AS POBDC_QT"," OP.OPERADOR_ID as OPERADOR_ID_OP","OU.OPERADOR_ID as OPERADOR_ID_OU","COALESCE(OP.OPERADOR_ID, OU.OPERADOR_ID, 0) AS OPERADOR_ID"," CASE WHEN OP.OPERADOR_ID IS NOT NULL THEN OP.PORCENTAJE_QT ELSE OU.PORCENTAJE_QT END as PORCENTAJE_QT","coalesce(OP.UTE_ID,0) AS UTE_ID"," CASE WHEN OP.UTE_ID IS NOT NULL THEN OP.PORCENTAJE_QT ELSE NULL END AS PORCENTAJE_UTE_QT","OP.DESDE_DT","OP.HASTA_DT","OP.UFUGA_ID","OP.MEDIOSPP_SN").as("OP").join(dataframes("F").as("UF2"),col("UF2.UFUGA_ID")===col("OP.UFUGA_ID"),"right")

      }
      val TF =  dataframes("TPL").filter((dataframes("TPL")("DESDE_DT")
        .between("2017-07-01","2017-07-31")) || (dataframes("TPL")("DESDE_DT")
        .lt(lit ("2017-07-01")) && (dataframes("TPL")("HASTA_DT").gt(lit("2017-07-01")) || dataframes("TPL")("HASTA_DT")
        .isNull))).toDF
      val jointf= TF.join(dataframes("R").as("R"),TF("ELMUN_ID") === col("R.ELMUN_ID"), "right")

      //Revisar coalesce
      val UFjoin =  dataframes("F").join(dataframes("U").as("U"),dataframes("F")("UGACT_ID") === col("U.UAACT_ID"), "left")
        .join(dataframes("E"),dataframes("F")("DESDE_DT") <=  dataframes("E")("HASTA_DT") &&
          coalesce(dataframes("F")("HASTA_DT"),dataframes("E")("DESDE_DT")) >= dataframes("E")("DESDE_DT"),"left")
        .join(dataframes("V2").as("V2"),dataframes("F")("UNFAC_ID") === dataframes("V2")("PROVE_ID"), "right" )

      dataframes+= "UF2"->OPtable()
      dataframes+= "UF"->UFjoin
      dataframes+= "R"->jointf
    }
    def filtrosF():Unit={
      dataframes("E")=
        dataframes("E").where("DESDE_DT between cast('"+"2017-07-01"+"' as date) and cast('"+"2017-07-31"+"' as date)")
      val max=dataframes("E").as("E").join(dataframes("E").as("P2"),col("E.UFTRG_ID")===col("P2.UFTRG_ID")&&col("P2.DESDE_DT")===col("E.DESDE_DT")).selectExpr("MAX(P2.VERSI_ID)").take(1).apply(0).getString(0).toInt
      dataframes("E")=dataframes("E").as("E").where(col("E.VERSI_ID")===max)
      dataframes("U")=dataframes("U").where("ACTIV_ID IN (1,2)");
      dataframes("G")= dataframes("G").filter((dataframes("G")("DESDE_DT").gt(lit("2017-07-01"))
        && (dataframes("G")("DESDE_DT").lt(lit("2017-07-31"))))
        || (dataframes("G")("DESDE_DT").lt(lit("2017-07-01"))
        && (dataframes("G")("HASTA_DT").gt(lit("2017-07-01"))|| dataframes("G")("HASTA_DT").isNull)))


      dataframes("M")= dataframes("M").filter((dataframes("M")("DESDE_DT").gt(lit("2017-07-01"))
        && (dataframes("M")("DESDE_DT").lt(lit("2017-07-31"))))
        || (dataframes("M")("DESDE_DT").lt(lit("2017-07-01"))
        && (dataframes("M")("HASTA_DT").gt(lit("2017-07-01"))
        || dataframes("M")("HASTA_DT").isNull)))


      dataframes("T")= dataframes("T").filter((dataframes("T")("DESDE_DT").gt(lit("2017-07-01"))
        && (dataframes("T")("DESDE_DT").lt(lit("2017-07-31"))))
        || (dataframes("T")("DESDE_DT").lt(lit("2017-07-01"))
        && (dataframes("T")("HASTA_DT").gt(lit("2017-07-01"))|| dataframes("T")("HASTA_DT").isNull)))

      dataframes("R") = dataframes("R").filter((col("R.DESDE_DT").gt(lit("2017-07-01"))
        && (col("R.DESDE_DT").lt(lit("2017-07-31")))
        || (col("R.DESDE_DT").lt(lit("2017-07-01")))
        && (col("R.HASTA_DT").gt(lit("2017-07-01")))
        || col("R.HASTA_DT").isNull))


}
    def joins():DataFrame={
      dataframes("F")=dataframes("F").join(dataframes("V").as("V"),col("V.PROVE_ID")===col("UNFAC_ID"),"right")
      dataframes("F").show(10)
      val innerjoin = dataframes("U").join(dataframes("G"),dataframes("U")("UAACT_ID") === dataframes("G")("UAACT_ID"), "inner")
      val innerGM =   innerjoin.join(dataframes("M"),dataframes("G")("UGACT_ID") === dataframes("M")("UGACT_ID"), "inner")
      val joinML =  innerGM.join(dataframes("L").as("L"),dataframes("M")("ELMUN_ID") ===col("L.ELMUN_ID"), "inner")
      val joinFG =  joinML.join(dataframes("F"),dataframes("G")("UGACT_ID") === dataframes("F")("UGACT_ID"), "inner")
      val joinTF =   joinFG.join(dataframes("T").as("T"),dataframes("F")("UFUGA_ID") === col("T.UFUGA_ID"), "inner")
      joinTF.show(2)
      dataframes("R").show(3)
      val joinRT =  joinTF.join(dataframes("R"),col("T.MUNTR_ID") === col("R.MUNTR_ID"), "inner")
      val joinLR =  joinRT.where(col("R.ELMUN_ID") === col("L.ELMUN_ID"))
      val joinET =   joinLR.join(dataframes("E").as("E"),col("T.UFTRG_ID") === col("E.UFTRG_ID"), "inner")
      val joinFP = joinET
        .join(dataframes("P").as("P2"),dataframes("F")("UFUGA_ID") === col("P2.UFUGA_ID"), "inner")
        .where(dataframes("P")("DESDE_DT") === dataframes("E")("DESDE_DT"))

      val joinUA =   joinFP.join(dataframes("UA").as("UA"),col("UA.UNADM_ID") ===dataframes("U")("UNADM_ID"), "inner")
        .join(dataframes("C").as("C2"),col("UA.COMAU_ID") === col("C2.COMAU_ID"), "inner")
      val joinSL = joinUA.join(dataframes("S").as("S2"),col("S2.ELMUN_ID") === col("L.ELMUN_ID"),"inner")
      val joinTPR = joinSL.join(dataframes("TP").as("TP"),col("TP.TPREC_ID") === dataframes("R")("TPREC_ID"),"inner")
      val joinUF2P =  joinTPR.join(dataframes("UF2"),col("UF2.UFUGA_ID") === col("P2.UFUGA_ID"))
      val joinUF =  joinUF2P.join(dataframes("UF").as("UF"),col("UF.UFUGA_ID") === col("UF2.UFUGA_ID"))
      val joinV2V = joinUF.where(col("UF.PROVE_ID") === col("V.PROVE_ID"))
      joinV2V
    }

    from()
    filtrosF()
    val dfJoin=joins()
    val filtT = dfJoin.filter(col("T.DESDE_DT").lt(col("E.DESDE_DT"))
      && (col("T.HASTA_DT").gt(col("E.HASTA_DT")))
      || col("T.HASTA_DT").isNull)
      .filter((dataframes("S")("DESDE_DT").lt(col("E.DESDE_DT"))
      && (dataframes("S")("HASTA_DT").gt(col("E.HASTA_DT")))
      || dataframes("S")("HASTA_DT").isNull))
    //filtT.take(3)
    print("fin")
}

def CargaKilos():Unit={}
//creacion de la tabla kilos


}
