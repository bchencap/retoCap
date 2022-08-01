package ingestion

import Recogida.spark
import org.apache.spark.sql.functions.lit

import java.util.Date

//import Recogida{spark, sparkContext}
object TransformacionesLocal extends {

  def RecogidaInicial(): Unit = {

    val VM_UAACTIVI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UAACTIVI_20220624_174209.csv.bz2")
    val VM_UGACTMUN = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UGACTMUN_20220624_174425.csv.bz2")
    val VM_ENTLOCAL = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ENTLOCAL_20220624_171535.csv.bz2")
    val VM_UFUGACTI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UFUGACTI_20220624_174339.csv.bz2")
    val VM_UFTRGMUN = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UFTRGMUN_20220624_174447.csv.bz2")
    val VM_ENTLTPRE = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ENTLTPRE_20220624_172757.csv.bz2")
    val VM_POBPERST = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_POBPERST_20220624_172950.csv.bz2")
    val VM_UGACTIVI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UGACTIVI_20220624_174307.csv.bz2")
    val VM_ELTREPOB = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ELTREPOB_20220624_172746.csv.bz2")
    val VM_TIPOLENT = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TIPOLENT_20220624_174014.csv.bz2")
    val VM_COMUAUTO = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_COMUAUTO_20220624_171044.csv.bz2")
    val VM_TPRECOGI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TPRECOGI_20220624_174341.csv.bz2")
    val VM_UNIDADMI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UNIDADMI_20220624_174438.csv.bz2")
    val VM_TIPOLFAC = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TIPOLFAC_20220624_174344.csv.bz2")
    val SGE_SAP_PROVEEDORES = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGE_SAP_PROVEEDORES_20220624_165906.csv.bz2")
    val SGR_MU_ASIG_OPERADORES_UF_TMP = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGR_MU_ASIG_OPERADORES_UF_TMP_20220624_165920.csv.bz2")
    val SGR_MU_ASIG_OPERADORES_UTE_TMP = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP_20220624_165929.csv.bz2")

    /*
    VM_UAACTIVI.show(5)
    VM_UGACTMUN.show(5)
    VM_ENTLOCAL.show(5)
    VM_UFUGACTI.show(5)
    VM_UFTRGMUN.show(5)
    VM_ENTLTPRE.show(5)
    VM_POBPERST.show(5)
    VM_UGACTIVI.show(5)
    VM_ELTREPOB.show(5)
    VM_TIPOLENT.show(5)
    VM_COMUAUTO.show(5)
    VM_TPRECOGI.show(5)
    VM_UNIDADMI.show(5)
    VM_TIPOLFAC.show(5)
    SGE_SAP_PROVEEDORES.show(5)
    SGR_MU_ASIG_OPERADORES_UF_TMP.show(5)
    SGR_MU_ASIG_OPERADORES_UTE_TMP.show(5)
     */

    /*val VM_ELTREPOB2 = VM_ELTREPOB.filter(VM_ELTREPOB("DESDE_DT").lt(lit("2017-08-01")) && (VM_ELTREPOB("DESDE_DT").gt(lit("2017-06-30")))).show()
    val VM_ELTREPOB3 = VM_ELTREPOB.filter(VM_ELTREPOB("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_ELTREPOB4 = VM_ELTREPOB.filter(VM_ELTREPOB("HASTA_DT").gt(lit("2017-06-30")) || VM_ELTREPOB("HASTA_DT").isNotNull).show()

    val VM_UAACTIVI2 = VM_UAACTIVI.filter(VM_UAACTIVI("DESDE_DT").lt(lit("2017-08-01")) && VM_UAACTIVI("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_UAACTIVI3 = VM_UAACTIVI.filter(VM_UAACTIVI("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_UAACTIVI4 = VM_UAACTIVI.filter(VM_UAACTIVI("HASTA_DT").gt(lit("2017-06-30")) || VM_UAACTIVI("HASTA_DT").isNotNull).show()

    val VM_UGACTIVI2 = VM_UGACTIVI.filter(VM_UGACTIVI("DESDE_DT").lt(lit("2017-08-01")) && VM_UGACTIVI("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_UGACTIVI3 = VM_UGACTIVI.filter(VM_UGACTIVI("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_UGACTIVI4 = VM_UGACTIVI.filter(VM_UGACTIVI("HASTA_DT").gt(lit("2017-06-30")) || VM_UGACTIVI("HASTA_DT").isNotNull).show()

    val VM_UGACTMUN2 = VM_UGACTMUN.filter(VM_UGACTMUN("DESDE_DT").lt(lit("2017-08-01")) && VM_UGACTMUN("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_UGACTMUN3 = VM_UGACTMUN.filter(VM_UGACTMUN("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_UGACTMUN4 = VM_UGACTMUN.filter(VM_UGACTMUN("HASTA_DT").gt(lit("2017-06-30")) || VM_UGACTMUN("HASTA_DT").isNotNull).show()

    val VM_UFUGACTI2 = VM_UFUGACTI.filter(VM_UFUGACTI("DESDE_DT").lt(lit("2017-08-01")) && VM_UFUGACTI("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_UFUGACTI3 = VM_UFUGACTI.filter(VM_UFUGACTI("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_UFUGACTI4 = VM_UFUGACTI.filter(VM_UFUGACTI("HASTA_DT").gt(lit("2017-06-30")) || VM_UFUGACTI("HASTA_DT").isNotNull).show()

    val VM_UFTRGMUN2 = VM_UFTRGMUN.filter(VM_UFTRGMUN("DESDE_DT").lt(lit("2017-08-01")) && VM_UFTRGMUN("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_UFTRGMUN3 = VM_UFTRGMUN.filter(VM_UFTRGMUN("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_UFTRGMUN4 = VM_UFTRGMUN.filter(VM_UFTRGMUN("HASTA_DT").gt(lit("2017-06-30")) || VM_UFTRGMUN("HASTA_DT").isNotNull).show()

    val VM_ENTLTPRE2 = VM_ENTLTPRE.filter(VM_ENTLTPRE("DESDE_DT").lt(lit("2017-08-01")) && VM_ENTLTPRE("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_ENTLTPRE3 = VM_ENTLTPRE.filter(VM_ENTLTPRE("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_ENTLTPRE4 = VM_ENTLTPRE.filter(VM_ENTLTPRE("HASTA_DT").gt(lit("2017-06-30")) || VM_ENTLTPRE("HASTA_DT").isNotNull).show()

    val VM_POBPERST2 = VM_POBPERST.filter(VM_POBPERST("DESDE_DT").lt(lit("2017-08-01")) && VM_POBPERST("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_POBPERST3 = VM_POBPERST.filter(VM_POBPERST("DESDE_DT").lt(lit("2017-07-01"))).show()
    // VM_POBPERST no tiene columna "HASTA_DT"

    val VM_TIPOLENT2 = VM_TIPOLENT.filter(VM_TIPOLENT("DESDE_DT").lt(lit("2017-08-01")) && VM_TIPOLENT("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_TIPOLENT3 = VM_TIPOLENT.filter(VM_TIPOLENT("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_TIPOLENT4 = VM_TIPOLENT.filter(VM_TIPOLENT("HASTA_DT").gt(lit("2017-06-30")) || VM_TIPOLENT("HASTA_DT").isNotNull).show()

    val VM_TIPOLFAC2 = VM_TIPOLFAC.filter(VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30"))).show()
    val VM_TIPOLFAC3 = VM_TIPOLFAC.filter(VM_TIPOLFAC("DESDE_DT").lt(lit("2017-07-01"))).show()
    val VM_TIPOLFAC4 = VM_TIPOLFAC.filter(VM_TIPOLFAC("HASTA_DT").gt(lit("2017-06-30")) || VM_TIPOLFAC("HASTA_DT").isNotNull).show()

    val SGE_SAP_PROVEEDORES2 = SGE_SAP_PROVEEDORES.filter(SGE_SAP_PROVEEDORES("EXENTO_DESDE_DT").lt(lit("2017-08-01")) && SGE_SAP_PROVEEDORES("EXENTO_DESDE_DT").gt(lit("2017-06-30")))
    val SGE_SAP_PROVEEDORES3 = SGE_SAP_PROVEEDORES.filter(SGE_SAP_PROVEEDORES("EXENTO_DESDE_DT").lt(lit("2017-07-01"))).show()
    val SGE_SAP_PROVEEDORES4 = SGE_SAP_PROVEEDORES.filter(SGE_SAP_PROVEEDORES("EXENTO_HASTA_DT").gt(lit("2017-06-30")) || SGE_SAP_PROVEEDORES("EXENTO_HASTA_DT").isNotNull)

    val SGR_MU_ASIG_OPERADORES_UF_TMP2 = SGR_MU_ASIG_OPERADORES_UF_TMP.filter(SGR_MU_ASIG_OPERADORES_UF_TMP("DESDE_DT").lt(lit("2017-08-01")) && SGR_MU_ASIG_OPERADORES_UF_TMP("DESDE_DT").gt(lit("2017-06-30")))
    val SGR_MU_ASIG_OPERADORES_UF_TMP3 = SGR_MU_ASIG_OPERADORES_UF_TMP.filter(SGR_MU_ASIG_OPERADORES_UF_TMP("DESDE_DT").lt(lit("2017-07-01"))).show()
    val SGR_MU_ASIG_OPERADORES_UF_TMP4 = SGR_MU_ASIG_OPERADORES_UF_TMP.filter(SGR_MU_ASIG_OPERADORES_UF_TMP("HASTA_DT").gt(lit("2017-06-30")) || SGR_MU_ASIG_OPERADORES_UF_TMP("HASTA_DT").isNotNull)
*/

    val VM_UAACTIVI2 = VM_UAACTIVI.filter((VM_UAACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (VM_UAACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (VM_UAACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (VM_UAACTIVI("HASTA_DT").gt(lit("2017-07-01")) || VM_UAACTIVI("HASTA_DT").isNull)))

    val VM_ELTREPOB2 = VM_ELTREPOB.filter((VM_ELTREPOB("DESDE_DT").lt(lit("2017-08-01")) && (VM_ELTREPOB("DESDE_DT").gt(lit("2017-06-30")))) || (VM_ELTREPOB("DESDE_DT").lt(lit("2017-07-01")) && (VM_ELTREPOB("HASTA_DT").gt(lit("2017-07-01")) || VM_ELTREPOB("HASTA_DT").isNull)))

    val VM_UGACTIVI2 = VM_UGACTIVI.filter((VM_UGACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (VM_UGACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (VM_UGACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (VM_UGACTIVI("HASTA_DT").gt(lit("2017-07-01")) || VM_UGACTIVI("HASTA_DT").isNull)))

    val VM_UGACTMUN2 = VM_UGACTMUN.filter((VM_UGACTMUN("DESDE_DT").lt(lit("2017-08-01")) && (VM_UGACTMUN("DESDE_DT").gt(lit("2017-06-30")))) || (VM_UGACTMUN("DESDE_DT").lt(lit("2017-07-01")) && (VM_UGACTMUN("HASTA_DT").gt(lit("2017-07-01")) || VM_UGACTMUN("HASTA_DT").isNull)))

    val VM_UFUGACTI2 = VM_UFUGACTI.filter((VM_UFUGACTI("DESDE_DT").lt(lit("2017-08-01")) && (VM_UFUGACTI("DESDE_DT").gt(lit("2017-06-30")))) || (VM_UFUGACTI("DESDE_DT").lt(lit("2017-07-01")) && (VM_UFUGACTI("HASTA_DT").gt(lit("2017-07-01")) || VM_UFUGACTI("HASTA_DT").isNull)))

    val VM_UFTRGMUN2 = VM_UFTRGMUN.filter((VM_UFTRGMUN("DESDE_DT").lt(lit("2017-08-01")) && (VM_UFTRGMUN("DESDE_DT").gt(lit("2017-06-30")))) || (VM_UFTRGMUN("DESDE_DT").lt(lit("2017-07-01")) && (VM_UFTRGMUN("HASTA_DT").gt(lit("2017-07-01")) || VM_UFTRGMUN("HASTA_DT").isNull)))

    val VM_ENTLTPRE2 = VM_ENTLTPRE.filter((VM_ENTLTPRE("DESDE_DT").lt(lit("2017-08-01")) && (VM_ENTLTPRE("DESDE_DT").gt(lit("2017-06-30")))) || (VM_ENTLTPRE("DESDE_DT").lt(lit("2017-07-01")) && (VM_ENTLTPRE("HASTA_DT").gt(lit("2017-07-01")) || VM_ENTLTPRE("HASTA_DT").isNull)))

    val VM_TIPOLFAC2 = VM_TIPOLFAC.filter((VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && (VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30")))) || (VM_TIPOLFAC("DESDE_DT").lt(lit("2017-07-01")) && (VM_TIPOLFAC("HASTA_DT").gt(lit("2017-07-01")) || VM_TIPOLFAC("HASTA_DT").isNull)))


    VM_UFUGACTI.join(SGE_SAP_PROVEEDORES, VM_UFUGACTI("UNFAC_ID") === SGE_SAP_PROVEEDORES("PROVE_ID"), "right") // línea 20

    VM_TIPOLFAC2.join(VM_ENTLTPRE, VM_TIPOLFAC2("ELMUN_ID") === VM_ENTLTPRE("ELMUN_ID"), "right") // líneas 27-31

    val UFU_UGAC = VM_UFUGACTI.join(VM_UGACTIVI, VM_UFUGACTI("UGACT_ID") === VM_UGACTIVI("UGACT_ID"), "left") // línea 32

    //UFU_UGAC.join(VM_ELTREPOB, VM_UFUGACTI("DESDE_DT") <= VM_ELTREPOB("HASTA_DT") && (VM_UFUGACTI("HASTA_DT") && VM_ELTREPOB("DESDE_DT")) >= VM_ELTREPOB("DESDE_DT"), "left").show()

    //UFU_UGAC.join(VM_ELTREPOB, VM_UFUGACTI("DESDE_DT").lt(VM_ELTREPOB("HASTA_DT")) && (VM_UFUGACTI("HASTA_DT") && VM_ELTREPOB("DESDE_DT"))..lt(VM_ELTREPOB("DESDE_DT")), "left").show()
    println(UFU_UGAC.getClass)


    //VM_UAACTIVI2.write.option("header", "true").csv("output.csv")
    //UFU_UGAC.write.option("header", "true").csv("output.csv")

  }
}
