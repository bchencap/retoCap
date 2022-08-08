package Recogida.Common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, coalesce ,to_timestamp}
import org.apache.spark.storage.StorageLevel

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
   // print(dataframes.apply("C").take(10).apply(0).toString())
   //val TF =  dataframes("TPL").filter((dataframes("TPL")("DESDE_DT").between("2017-07-01","2017-07-31")) || (dataframes("TPL")("DESDE_DT").lt(lit ("2017-07-01")) && (dataframes("TPL")("HASTA_DT").gt(lit("2017-07-01")) || (dataframes("TPL")("HASTA_DT").isNull)))).join(dataframes("R"),col("ELMUN_ID") === dataframes("R")("ELMUN_ID"), "right")
   //val UFjoin =  dataframes("F").join(dataframes("U"),dataframes("F")("UGACT_ID") === dataframes("U")("UGACT_ID"), "left")
   //  .join(dataframes("E"),dataframes("F")("DESDE_DT") <=  dataframes("E")("HASTA_DT") && (dataframes("F")("HASTA_DT"), dataframes("E")("DESDE_DT").isNull >= dataframes("E")("DESDE_DT")))
   //  .join(dataframes("DWE_SGE_SAP_PROVEEDORES"),dataframes("F")("UNFAC_ID") === dataframes("DWE_SGE_SAP_PROVEEDORES")("PROVE_ID"), "right" )

    //
    //dataframes("F").join(dataframes("E"),dataframes("F")("DESDE_DT") <=  dataframes("E")("HASTA_DT") && (dataframes("F")("HASTA_DT"), dataframes("E")("DESDE_DT").isNull >= dataframes("E")("DESDE_DT")))
    // V2 = DWE_SGE_SAP_PROVEEDORES
    //dataframes("F").join(dataframes("DWE_SGE_SAP_PROVEEDORES"),dataframes("F")("UNFAC_ID") === dataframes("DWE_SGE_SAP_PROVEEDORES")("PROVE_ID"), "right" )
    //TF.show(5)

    //VM_UAACTIVI.show(5)

    val TF =  dataframes("TPL").filter((dataframes("TPL")("DESDE_DT")
      .between("2017-07-01","2017-07-31")) || (dataframes("TPL")("DESDE_DT")
      .lt(lit ("2017-07-01")) && (dataframes("TPL")("HASTA_DT").gt(lit("2017-07-01")) || dataframes("TPL")("HASTA_DT")
      .isNull))).toDF
    val jointf= TF.join(dataframes("R"),TF("ELMUN_ID") === dataframes("R")("ELMUN_ID"), "right")
    val UFjoin =  dataframes("F").join(dataframes("U"),dataframes("F")("UGACT_ID") === dataframes("U")("UAACT_ID"), "left")
      .join(dataframes("E"),dataframes("F")("DESDE_DT") <=  dataframes("E")("HASTA_DT") && coalesce(dataframes("F")("HASTA_DT"),dataframes("E")("DESDE_DT")) >= dataframes("E")("DESDE_DT"))
      .join(dataframes("V2"),dataframes("F")("UNFAC_ID") === dataframes("V2")("PROVE_ID"), "right" )


    //Inner-join

    val innerjoin = dataframes("U").join(dataframes("G"),dataframes("U")("UAACT_ID") === dataframes("G")("UAACT_ID"), "inner")
    val innerGM =   innerjoin.join(dataframes("M"),dataframes("G")("UGACT_ID") === dataframes("M")("UGACT_ID"), "inner")
    val joinML =  innerGM.join(dataframes("L"),dataframes("M")("ELMUN_ID") === dataframes("L")("ELMUN_ID"), "inner")
    val joinFG =  dataframes("G").join(dataframes("F"),dataframes("G")("UGACT_ID") === dataframes("F")("UGACT_ID"), "inner")
    val joinTF =   joinFG.join(dataframes("T"),dataframes("F")("UFUGA_ID") === dataframes("T")("UFUGA_ID"), "inner")
    val joinRT =  joinTF.join(dataframes("R"),dataframes("T")("MUNTR_ID") === dataframes("R")("MUNTR_ID"), "inner")
    val joinLR =  joinRT.join(dataframes("L"),dataframes("R")("ELMUN_ID") === dataframes("L")("ELMUN_ID"), "inner")
    val joinET =   dataframes("T").join(dataframes("E"),dataframes("T")("UFTRG_ID") === dataframes("E")("UFTRG_ID"), "inner")
    val joinFP = dataframes("F").join(dataframes("P"),dataframes("F")("UFUGA_ID") === dataframes("P")("UFUGA_ID"), "inner")
      .join(dataframes("E"),dataframes("P")("DESDE_DT") === dataframes("E")("DESDE_DT"), "inner")
    val joinUA =   dataframes("UA").join(dataframes("U"),dataframes("UA")("UNADM_ID") === dataframes("U")("UNADM_ID"), "inner")
      .join(dataframes("C"),dataframes("UA")("COMAU_ID") === dataframes("C")("COMAU_ID"), "inner")
    val joinSL = dataframes("S").join(dataframes("L"),dataframes("S")("ELMUN_ID") === dataframes("L")("ELMUN_ID"),"inner")
    val joinTPR = dataframes("TP").join(dataframes("R"),dataframes("TP")("TPREC_ID") === dataframes("R")("TPREC_ID"),"inner")
    val joinV2V = dataframes("V2").join(dataframes("V"),dataframes("V2")("PROVE_ID") === dataframes("V")("PROVE_ID"), "inner")
    val joinUF2P = dataframes("F").join(dataframes("P"),dataframes("F")("UFUGA_ID") === dataframes("P")("UFUGA_ID"), "inner")
    val joinUF =  dataframes("F").as("UF2").join(dataframes("F").as("UF"),col("UF.UGACT_ID") === col("UF2.UGACT_ID"))
    //joinUF.show(5)


    //Filtros
    val filtG= dataframes("G").filter((dataframes("G")("DESDE_DT").gt(lit("2017-07-01"))
      && (dataframes("G")("DESDE_DT").lt(lit("2017-07-31"))))
      || (dataframes("G")("DESDE_DT").lt(lit("2017-07-01"))
      && (dataframes("G")("HASTA_DT").gt(lit("2017-07-01"))|| dataframes("G")("HASTA_DT").isNull)))


    val filtMjul01_31= dataframes("M")filter((dataframes("M")("DESDE_DT").gt(lit("2017-07-01"))
      && (dataframes("M")("DESDE_DT").lt(lit("2017-07-31"))))
      || (dataframes("M")("DESDE_DT").lt(lit("2017-07-01"))
      && (dataframes("M")("HASTA_DT").gt(lit("2017-07-01"))
      || dataframes("M")("HASTA_DT").isNull)))


    val TfiltJUL01_31= dataframes("T").filter((dataframes("T")("DESDE_DT").gt(lit("2017-07-01"))
      && (dataframes("T")("DESDE_DT").lt(lit("2017-07-31"))))
      || (dataframes("T")("DESDE_DT").lt(lit("2017-07-01"))
      && (dataframes("T")("HASTA_DT").gt(lit("2017-07-01"))|| dataframes("T")("HASTA_DT").isNull)))

    val filtR = dataframes("R").filter((dataframes("R")("DESDE_DT").gt(lit("2017-07-01"))
      && (dataframes("R")("DESDE_DT").lt(lit("2017-07-31")))
      || (dataframes("R")("DESDE_DT").lt(lit("2017-07-01")))
      && (dataframes("R")("HASTA_DT").gt(lit("2017-07-01")))
      || dataframes("R")("HASTA_DT").isNull))

    //Error por que no se he hecho join aún
    val filtT = dataframes("T").as("T2").filter(col("T2.DESDE_DT").lt(dataframes("E").as("E2")("E2.DESDE_DT"))
      && (col("T2.HASTA_DT").gt(dataframes("E").as("E2")("E2.HASTA_DT")))
      || col("T2.HASTA_DT").isNull)

    val filtS = dataframes("S").filter((dataframes("S")("DESDE_DT").lt(dataframes("E")("DESDE_DT"))
      && (dataframes("S")("HASTA_DT").gt(dataframes("E")("HASTA_DT")))
      || dataframes("S")("HASTA_DT").isNull))

    filtT.show(5)
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilos


}
