package Recogida.Common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{coalesce, col, lit, to_timestamp, when}
import org.apache.spark.storage.StorageLevel

import java.io.File
import scala.collection.mutable.Map
import java.sql.Timestamp
import java.util.Date


object TransformacionesLocal extends Common {
  def extractFilePath(name:String):String={
    val format = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")

    val file = new File("./")
    val files=file.listFiles.filter(_.isFile)
    .filter(_.getName.startsWith(name))
    .map((x)=> x).toList.sortBy((x)=>format.parse(x.getName.substring(name.length,x.getName.indexOf('.'))).getTime)(Ordering[Long].reverse)

    return files(0).getName;
  }
  val readFiles=Map(
    "V2"->("DWE_SGE_SAP_PROVEEDORES_","PROVE_NM,PROVE_ID"),
    "V"->("DWE_SGE_SAP_PROVEEDORES_","PROVE_NM,PROVE_ID"),
    "DWE_SGR_MU_ASIG_OPERADORES_UF"->("DWE_SGR_MU_ASIG_OPERADORES_UF_TMP_","UTE_ID,PORCENTAJE_QT,DESDE_DT,HASTA_DT,UFUGA_ID,MEDIOSPP_SN,OPERADOR_ID"),
    "DWE_SGR_MU_ASIG_OPERADORES_UTE"->("DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP_","UTE_ID,PORCENTAJE_QT,OPERADOR_ID"),
    //DWE_VM_COMUAUTO
    "C"->("DWE_VM_COMUAUTO_","COMAU_ID"),
    //DWE_VM_ELTREMED NO SE USA
    //"ELTREMED"->"DWE_VM_ELTREMED_",
    //DWE_VM_ELTREPOB // en algun lado está como P2
    "E"->("DWE_VM_ELTREPOB_","VERSI_ID,UFTRG_ID,DESDE_DT,HASTA_DT,POBIN_QT,POBLA_QT"),
    //DWE_VM_ENTLOCAL
    "L"->("DWE_VM_ENTLOCAL_","ELMUN_ID"),
    //DWE_VM_ENTLTPRE
    "R"->("DWE_VM_ENTLTPRE_","ELMUN_ID,TPREC_ID,DESDE_DT,HASTA_DT,MUNTR_ID"),
    //DWE_VM_POBPERST
    "P"->("DWE_VM_POBPERST_","UFUGA_ID,DESDE_DT"),
    //DWE_VM_TIPOLENT
    "S"->("DWE_VM_TIPOLENT_","ELMUN_ID,DESDE_DT,HASTA_DT,TPENT_ID"),
    //DWE_VM_TIPOLFAC está como TP
    "TPL"->("DWE_VM_TIPOLFAC_","DESDE_DT,HASTA_DT,ELMUN_ID,TPGFA_ID"),
    //DWE_VM_TPRECOGI
    "TP"->("DWE_VM_TPRECOGI_","TPREC_ID,PROCE_ID"),
    //DWE_VM_UAACTIVI
    "U"->("DWE_VM_UAACTIVI_","UNADM_ID,ACTIV_ID,UAACT_ID"),
    //DWE_VM_UFTRGMUN
    "T"->("DWE_VM_UFTRGMUN_","UFUGA_ID,MUNTR_ID,UFTRG_ID,DESDE_DT,HASTA_DT"),
    //DWE_VM_UFUGACTI TAMBIÉN ESTÁ COMO UF Y UF2
    "F"->("DWE_VM_UFUGACTI_","UGACT_ID,UFUGA_ID,UNFAC_ID,DESDE_DT,HASTA_DT"),
    //DWE_VM_UGACTIVI
    "G"->("DWE_VM_UGACTIVI_","UAACT_ID,UGACT_ID,DESDE_DT,HASTA_DT,UNGES_ID"),
    //DWE_VM_UGACTMUN
    "M"->("DWE_VM_UGACTMUN_","UGACT_ID,ELMUN_ID,DESDE_DT,HASTA_DT"),
    //DWE_VM_UNIDADMI
    "UA"->("DWE_VM_UNIDADMI_","UNADM_ID,COMAU_ID"),
    //DWE_VM_VOLUMENS NO SE USA
    //"VOL"->"DWE_VM_VOLUMENS_"
  )
  var dataframes=Map[String,(DataFrame)]()
  def leerDatos(): Unit ={
    readFiles.foreach(
      (key)=>{
        dataframes+= key._1 -> spark.read.option("header","true").csv(extractFilePath(key._2._1)).selectExpr(key._2._2.split(","):_*).cache()
      }
    )
  }
  def RecogidaInicial(date: String): Unit = {
    def from(): Unit ={
      def OPtable():DataFrame={
        dataframes("DWE_SGR_MU_ASIG_OPERADORES_UTE").alias("OU").join(dataframes("DWE_SGR_MU_ASIG_OPERADORES_UF").alias("OP"),col("OU.UTE_ID")===col("OP.UTE_ID"),"right")
          .selectExpr(
            "(1 * coalesce(OP.PORCENTAJE_QT, 100) / 100) * coalesce(OU.PORCENTAJE_QT, 100) / 100 AS POBDC_QT",
            "COALESCE(OP.OPERADOR_ID, OU.OPERADOR_ID, 0) AS OPERADOR_ID",
            " CASE WHEN OP.OPERADOR_ID IS NOT NULL THEN OP.PORCENTAJE_QT ELSE OU.PORCENTAJE_QT END as PORCENTAJE_QT",
            "coalesce(OP.UTE_ID,0) AS UTE_ID",
            " CASE WHEN OP.UTE_ID IS NOT NULL THEN OP.PORCENTAJE_QT ELSE NULL END AS PORCENTAJE_UTE_QT","OP.DESDE_DT","OP.HASTA_DT",
            "OP.UFUGA_ID","OP.MEDIOSPP_SN").as("OP").join(dataframes("F").as("UF2"),col("UF2.UFUGA_ID")===col("OP.UFUGA_ID"),"right").selectExpr("OP.MEDIOSPP_SN","UF2.UFUGA_ID","UF2.UGACT_ID","OPERADOR_ID","PORCENTAJE_QT","UTE_ID","PORCENTAJE_UTE_QT","POBDC_QT")

      }
      filtrosF()
      val TF =  dataframes("TPL").filter((dataframes("TPL")("DESDE_DT")
        .between("2017-07-01","2017-07-31")) || (dataframes("TPL")("DESDE_DT")
        .lt(lit ("2017-07-01")) && (dataframes("TPL")("HASTA_DT").gt(lit("2017-07-01")) || dataframes("TPL")("HASTA_DT")
        .isNull))).toDF.selectExpr("TPGFA_ID","ELMUN_ID")
      val jointf= TF.as("TF").join(dataframes("R").as("R"),col("TF.ELMUN_ID") === col("R.ELMUN_ID"), "right").selectExpr("R.ELMUN_ID","R.TPREC_ID","R.MUNTR_ID","TF.TPGFA_ID")
      //Revisar coalesce
      val UFjoin =  dataframes("F").as("F").join(dataframes("E").as("E"),col("F.DESDE_DT") <= col("E.HASTA_DT") &&
        coalesce(col("F.HASTA_DT"),col("E.DESDE_DT")) >= col("E.DESDE_DT"),"left").selectExpr("E.DESDE_DT","E.HASTA_DT","E.VERSI_ID","E.POBIN_QT","E.POBLA_QT","E.UFTRG_ID","F.UFUGA_ID","F.UGACT_ID","F.UNFAC_ID")
        .join(dataframes("V2").as("V2"),col("F.UNFAC_ID") === col("V2.PROVE_ID"), "right" )
        .join(dataframes("G").as("G"),col("F.UGACT_ID") === col("G.UGACT_ID"), "left").selectExpr("E.DESDE_DT","E.HASTA_DT","E.VERSI_ID","E.POBIN_QT","E.POBLA_QT","V2.PROVE_ID","V2.PROVE_NM","G.UAACT_ID","E.UFTRG_ID","F.UFUGA_ID","G.UGACT_ID","F.UNFAC_ID","G.UNGES_ID")

      dataframes+= "UF2"->OPtable()
      dataframes+= "UF"->UFjoin
      dataframes+= "R"->jointf
    }
    def filtrosF():Unit={
      dataframes("E")=
        dataframes("E").where("DESDE_DT between cast('"+"2017-07-01"+"' as date) and cast('"+"2017-07-31"+"' as date)")

      val max=dataframes("E").as("E").join(dataframes("E").as("P2"),col("E.UFTRG_ID")===col("P2.UFTRG_ID")&&col("P2.DESDE_DT")===col("E.DESDE_DT")).selectExpr("MAX(P2.VERSI_ID)").take(1).apply(0).getString(0).toInt
      dataframes("E")=dataframes("E").as("E").where(col("E.VERSI_ID")===max).drop("P2.UFTRG_ID","P2.DESDE_DT")
      dataframes("U")=dataframes("U").where("ACTIV_ID IN (1,2)");
      dataframes("G")= dataframes("G").filter((dataframes("G")("DESDE_DT").gt(lit("2017-07-01"))
        && (dataframes("G")("DESDE_DT").lt(lit("2017-07-31"))))
        || (dataframes("G")("DESDE_DT").lt(lit("2017-07-01"))
        && (dataframes("G")("HASTA_DT").gt(lit("2017-07-01"))|| dataframes("G")("HASTA_DT").isNull))).drop("DESDE_DT","HASTA_DT")


      dataframes("M")= dataframes("M").filter((dataframes("M")("DESDE_DT").gt(lit("2017-07-01"))
        && (dataframes("M")("DESDE_DT").lt(lit("2017-07-31"))))
        || (dataframes("M")("DESDE_DT").lt(lit("2017-07-01"))
        && (dataframes("M")("HASTA_DT").gt(lit("2017-07-01"))
        || dataframes("M")("HASTA_DT").isNull))).drop("DESDE_DT","HASTA_DT")


      dataframes("T")= dataframes("T").filter((dataframes("T")("DESDE_DT").gt(lit("2017-07-01"))
        && (dataframes("T")("DESDE_DT").lt(lit("2017-07-31"))))
        || (dataframes("T")("DESDE_DT").lt(lit("2017-07-01"))
        && (dataframes("T")("HASTA_DT").gt(lit("2017-07-01"))|| dataframes("T")("HASTA_DT").isNull)))

      dataframes("R") = dataframes("R").filter((col("DESDE_DT").gt(lit("2017-07-01"))
        && (col("DESDE_DT").lt(lit("2017-07-31")))
        || (col("DESDE_DT").lt(lit("2017-07-01")))
        && (col("HASTA_DT").gt(lit("2017-07-01")))
        || col("HASTA_DT").isNull))


}
    def joins():DataFrame={
      dataframes("F")=dataframes("F").as("F").join(dataframes("V").as("V"),col("V.PROVE_ID")===col("F.UNFAC_ID"),"right")
      //dataframes("F").show(10)
      val innerjoin = dataframes("U").as("U").join(dataframes("UF").as("UF"),col("U.UAACT_ID") === col("UF.UAACT_ID"), "inner").selectExpr("UF.PROVE_ID","UF.UNGES_ID","U.ACTIV_ID","UF.UGACT_ID","U.UNADM_ID","UF.UNGES_ID","UF.UFTRG_ID","UF.DESDE_DT","UF.HASTA_DT","UF.POBIN_QT","UF.POBLA_QT","UF.UFTRG_ID")
      val innerGM =   innerjoin.join(dataframes("M").as("M"),col("UF.UGACT_ID") === col("M.UGACT_ID"), "inner").drop("M.UGACT_ID")
      val joinML =  innerGM.join(dataframes("L").as("L"),col("M.ELMUN_ID") ===col("L.ELMUN_ID"), "inner").drop("M.ELMUN_ID")
      val joinFG =  joinML.join(dataframes("F"),col("UF.UGACT_ID") === col("F.UGACT_ID"),"inner").selectExpr("UF.UNGES_ID","UF.PROVE_ID","U.ACTIV_ID","L.ELMUN_ID","F.HASTA_DT","V.PROVE_ID","V.PROVE_NM","F.UFUGA_ID","UF.UGACT_ID","U.UNADM_ID","F.UNFAC_ID","UF.UFTRG_ID","UF.DESDE_DT","UF.HASTA_DT","UF.POBIN_QT","UF.POBLA_QT","UF.UFTRG_ID")
      val joinTF =   joinFG.join(dataframes("T").as("T"),col("F.UFUGA_ID") === col("T.UFUGA_ID"), "inner").drop("T.UFUGA_ID")
      val joinRT =  joinTF.join(dataframes("R"),col("T.MUNTR_ID") === col("R.MUNTR_ID"), "inner").drop("T.MUNTR_ID","R.MUNTR_ID")
      val joinLR =  joinRT.where(col("R.ELMUN_ID") === col("L.ELMUN_ID")).drop("R.ELMUN_ID")
      val joinET =   joinLR.where(col("T.UFTRG_ID") === col("UF.UFTRG_ID")).selectExpr("UF.UNGES_ID","UF.PROVE_ID","U.ACTIV_ID","T.DESDE_DT","L.ELMUN_ID","F.HASTA_DT","T.HASTA_DT","V.PROVE_ID","V.PROVE_NM","TF.TPGFA_ID","R.TPREC_ID","F.UFUGA_ID","UF.UGACT_ID","U.UNADM_ID","F.UNFAC_ID","UF.DESDE_DT","UF.HASTA_DT","UF.POBIN_QT","UF.POBLA_QT","UF.UFTRG_ID")
      val joinFP = joinET
        .join(dataframes("P").as("P2"),col("F.UFUGA_ID") === col("P2.UFUGA_ID"), "inner")
        .where(col("P2.DESDE_DT") === col("UF.DESDE_DT")).drop("F.UFUGA_ID","P.DESDE_DT")
      val joinUA =   joinFP.join(dataframes("UA").as("UA"),col("UA.UNADM_ID") ===col("U.UNADM_ID"), "inner").drop("UA.UNADM_ID")
        .join(dataframes("C").as("C2"),col("UA.COMAU_ID") === col("C2.COMAU_ID"), "inner").drop("UA.COMAU_ID","C.COMAU_ID")
      val joinSL = joinUA.join(dataframes("S").as("S2"),col("S2.ELMUN_ID") === col("L.ELMUN_ID"),"inner").drop("S2.ELMUN_ID").selectExpr("UF.UNGES_ID","UF.PROVE_ID","U.ACTIV_ID","S2.DESDE_DT","P2.DESDE_DT","T.DESDE_DT","S2.ELMUN_ID","L.ELMUN_ID","S2.HASTA_DT","F.HASTA_DT","T.HASTA_DT","V.PROVE_ID","V.PROVE_NM","S2.TPENT_ID","TF.TPGFA_ID","R.TPREC_ID","P2.UFUGA_ID","F.UFUGA_ID","UF.UGACT_ID","U.UNADM_ID","F.UNFAC_ID","UF.DESDE_DT","UF.HASTA_DT","UF.POBIN_QT","UF.POBLA_QT","UF.UFTRG_ID")
      val joinTPR = joinSL.join(dataframes("TP").as("TP"),col("TP.TPREC_ID") === col("R.TPREC_ID"),"inner").drop("TP.TPREC_ID")
      val joinUF2P =  joinTPR.join(dataframes("UF2"),col("UF2.UFUGA_ID") === col("P2.UFUGA_ID")).drop("P2.UFUGA_ID")
      val joinUF =  joinUF2P.where(col("F.UFUGA_ID") === col("UF2.UFUGA_ID")).drop("F.UFUGA_ID","UF2.UFUGA_ID").selectExpr("POBDC_QT","U.ACTIV_ID","UF.PROVE_ID","S2.DESDE_DT","P2.DESDE_DT","T.DESDE_DT","S2.ELMUN_ID","L.ELMUN_ID","S2.HASTA_DT","F.HASTA_DT","T.HASTA_DT","OP.MEDIOSPP_SN","OP.OPERADOR_ID","OP.PORCENTAJE_QT","OP.PORCENTAJE_UTE_QT","TP.PROCE_ID","V.PROVE_NM","S2.TPENT_ID","TF.TPGFA_ID","R.TPREC_ID","F.UFUGA_ID","UF2.UGACT_ID","U.UNADM_ID","F.UNFAC_ID","UF.UNGES_ID","OP.UTE_ID","UF.DESDE_DT","UF.HASTA_DT","UF.POBIN_QT","UF.POBLA_QT","UF.UFTRG_ID")
      val joinV2V = joinUF.where(col("UF.PROVE_ID") === col("V.PROVE_ID")).drop("UF.PROVE_ID","V.PROVE_ID")
      joinV2V
    }

    from()
    val dfJoin=joins()
    val filtT = dfJoin.filter(col("T.DESDE_DT").lt(col("UF.DESDE_DT"))
      && (col("T.HASTA_DT").gt(col("UF.HASTA_DT")))
      || col("T.HASTA_DT").isNull)
      .filter((col("S2.DESDE_DT").lt(col("UF.DESDE_DT"))
      && (col("S2.HASTA_DT").gt(col("UF.HASTA_DT")))
      || col("S2.HASTA_DT").isNull)).selectExpr(
      "UF.DESDE_DT", "U.UNADM_ID",
      "U.ACTIV_ID",
      "UF.UNGES_ID",
      "L.ELMUN_ID",
      "F.UNFAC_ID",
      "R.TPREC_ID",
      "S2.TPENT_ID",
      "coalesce( TF.TPGFA_ID,  S2.TPENT_ID,  TF.TPGFA_ID ) as TPGFA_ID",
      "coalesce(TP.PROCE_ID, 0, TP.PROCE_ID) as PROCE_ID",
      "V.PROVE_NM",
      "coalesce(OPERADOR_ID,0) as OPERADOR_ID",
      "PORCENTAJE_QT",
      "UF.POBIN_QT* coalesce(POBDC_QT,1) as POBDC_QT",
      "UF.POBLA_QT* coalesce(POBDC_QT,1) as POBGC_QT",
      "coalesce(UTE_ID,0) AS UTE_ID",
      "PORCENTAJE_UTE_QT",
      "OP.MEDIOSPP_SN").groupBy("UF.DESDE_DT", "U.UNADM_ID",
      "U.ACTIV_ID",
      "UF.UNGES_ID",
      "L.ELMUN_ID",
      "F.UNFAC_ID",
      "R.TPREC_ID",
      "S2.TPENT_ID",
      "TPGFA_ID",
      "PROCE_ID",
      "V.PROVE_NM",
      "OPERADOR_ID",
      "PORCENTAJE_QT",
      "UTE_ID",
      "PORCENTAJE_UTE_QT",
      "OP.MEDIOSPP_SN").sum("POBDC_QT","POBGC_QT").show(2)



    //.groupBy(when(col("OP.UTE_ID").isNull,0).otherwise(col("OP.UTE_ID"))).agg(functions.sum("E.POBIN_QT" ), functions.sum ("E.POBLA_QT"))
    /*  .groupBy("E.DESDE_DT" , "U.UNADM_ID" , "U.ACTIV_ID", "UF.UNGES_ID" , "L.ELMUN_ID" , "F.UNFAC_ID", "R.TPREC_ID" ,
    "S2.TPENT_ID" , "TF.TPGFA_ID" , "TP.PROCE_ID", "E.POBIN_QT", "E.POBLA_QT" , "OPERADOR_ID_OU", "OPERADOR_ID",
      "V.PROVE_NM","POBDC_QT","PORCENTAJE_QT", "PORCENTAJE_UTE_QT","OP.MEDIOSPP_SN" ).agg(functions.sum("E.POBIN_QT" ), functions.sum ("E.POBLA_QT"))
*/
(5)


    print("fin")
}


def CargaKilos():Unit={}
//creacion de la tabla kilos


}
