SELECT     E.DESDE_DT,
             U.UNADM_ID,
             U.ACTIV_ID,
             G.UNGES_ID,
             L.ELMUN_ID,
             F.UNFAC_ID,
             R.TPREC_ID,
             S.TPENT_ID,
             coalesce( TF.TPGFA_ID,  S.TPENT_ID,  TF.TPGFA_ID ) TPGFA_ID,
             coalesce(TP.PROCE_ID, 0, TP.PROCE_ID) PROCE_ID,
             V.PROVE_NM,coalesce(OPERADOR_ID,0) OPERADOR_ID,
            SUM(E.POBIN_QT) * coalesce(POBDC_QT,1) as POBDC_QT ,
             SUM(E.POBLA_QT) * coalesce(POBDC_QT,1) as POBGC_QT,
              PORCENTAJE_QT, -- PORCENTAJE DEL OPERADOR
              isnull(UTE_ID,0) AS UTE_ID,
             PORCENTAJE_UTE_QT,OP.MEDIOSPP_SN
      FROM DWE_VM_UAACTIVI U,
           DWE_VM_UGACTMUN M,
           DWE_VM_ENTLOCAL L,
           DWE_VM_UFUGACTI F right join  DWE_SGE_SAP_PROVEEDORES V on V.PROVE_ID = F.UNFAC_ID,
           DWE_VM_UFTRGMUN T,
           DWE_VM_POBPERST P,
           DWE_VM_TIPOLENT S,
           DWE_VM_COMUAUTO C,
           DWE_VM_TPRECOGI TP,
           DWE_VM_UNIDADMI UA ,
         (SELECT * FROM DWE_VM_TIPOLFAC TP
            WHERE (TP.DESDE_DT BETWEEN cast ('2017-07-01' as date )  AND cast ('2017-07-31' as date )
                    OR (TP.DESDE_DT <= cast ('2017-07-01' as date )
                    AND (TP.HASTA_DT >= cast ('2017-07-01' as date )  OR TP.HASTA_DT IS NULL)))) TF
                    right join  DWE_VM_ENTLTPRE R on TF.ELMUN_ID = R.ELMUN_ID,
         DWE_VM_UFUGACTI UF left join  DWE_VM_UGACTIVI G on UF.UGACT_ID = G.UGACT_ID--,
                            left join DWE_VM_ELTREPOB E on (UF.DESDE_DT <= E.HASTA_DT  and isnull(UF.HASTA_DT, E.DESDE_DT) >= E.DESDE_DT)
                            right join DWE_SGE_SAP_PROVEEDORES V2 on V2.PROVE_ID = uF.UNFAC_ID ,
             (
             select (1 * coalesce(OP.PORCENTAJE_QT, 100) / 100) * coalesce(OU.PORCENTAJE_QT, 100) / 100 AS POBDC_QT,
                       OP.OPERADOR_ID OPERADOR_ID_OP, OU.OPERADOR_ID OPERADOR_ID_OU,
             COALESCE(OP.OPERADOR_ID, OU.OPERADOR_ID, 0) AS OPERADOR_ID,
             CASE WHEN OP.OPERADOR_ID IS NOT NULL THEN OP.PORCENTAJE_QT
                  ELSE OU.PORCENTAJE_QT
             END PORCENTAJE_QT,
             coalesce(OP.UTE_ID,0) AS UTE_ID,
             CASE WHEN OP.UTE_ID IS NOT NULL THEN OP.PORCENTAJE_QT
                  ELSE NULL
             END AS PORCENTAJE_UTE_QT ,OP.DESDE_DT,OP.HASTA_DT, OP.UFUGA_ID,OP.MEDIOSPP_SN
             from DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP OU  left join DWE_SGR_MU_ASIG_OPERADORES_UF_TMP OP on OP.UTE_ID = OU.UTE_ID
             )  OP right join  DWE_VM_UFUGACTI UF2 on UF2.UFUGA_ID = OP.UFUGA_ID
      WHERE U.UAACT_ID = G.UAACT_ID
        AND G.UGACT_ID = M.UGACT_ID
        AND M.ELMUN_ID = L.ELMUN_ID
        AND G.UGACT_ID = F.UGACT_ID
        AND F.UFUGA_ID = T.UFUGA_ID
        AND T.MUNTR_ID = R.MUNTR_ID
        AND R.ELMUN_ID = L.ELMUN_ID
        AND T.UFTRG_ID = E.UFTRG_ID
        AND F.UFUGA_ID = P.UFUGA_ID
        AND P.DESDE_DT = E.DESDE_DT
        AND UA.UNADM_ID = U.UNADM_ID
        AND UA.COMAU_ID = C.COMAU_ID
        AND S.ELMUN_ID = L.ELMUN_ID
        AND TP.TPREC_ID = R.TPREC_ID
        AND V2.PROVE_ID= V.PROVE_ID
        and UF2.UFUGA_ID= P.UFUGA_ID
        and UF2.UGACT_ID =UF.UGACT_ID
        AND (G.DESDE_DT BETWEEN cast ('2017-07-01' as date )  AND cast ('2017-07-31' as date )
          OR (G.DESDE_DT <= cast ('2017-07-01' as date )
            AND (G.HASTA_DT >= cast ('2017-07-01' as date )  OR G.HASTA_DT IS NULL)))
        --Comprueba que la fecha de UGACTMUN incluya
        --el periodo especIFicado
        AND (M.DESDE_DT BETWEEN cast ('2017-07-01' as date )  AND cast ('2017-07-31' as date )
          OR (M.DESDE_DT <= cast ('2017-07-01' as date )
            AND (M.HASTA_DT >= cast ('2017-07-01' as date )  OR M.HASTA_DT IS NULL)))
        --Comprueba que la fecha de UFTRGMUN incluya
        --el periodo especIFicado
        AND (T.DESDE_DT BETWEEN cast ('2017-07-01' as date )  AND cast ('2017-07-31' as date )
          OR (T.DESDE_DT <= cast ('2017-07-01' as date )
           AND (T.HASTA_DT >= cast ('2017-07-01' as date )  OR T.HASTA_DT IS NULL)))
        --Comprueba que la fecha de ENTLTPRE incluya
        --el periodo especIFicado
        AND (R.DESDE_DT BETWEEN cast ('2017-07-01' as date )  AND cast ('2017-07-31' as date )
          OR (R.DESDE_DT <= cast ('2017-07-01' as date )
           AND (R.HASTA_DT >= cast ('2017-07-01' as date )  OR R.HASTA_DT IS NULL)))
        --Comprueba que la fecha de UFTRGMUN incluya
        --el periodo especIFicado en ELTREPOB
        AND (T.DESDE_DT <= E.DESDE_DT
            AND (T.HASTA_DT >= E.HASTA_DT OR T.HASTA_DT IS NULL))
        --Comprueba que la fecha de TIPOLENT
        AND (S.DESDE_DT <= E.DESDE_DT
            AND (S.HASTA_DT >= E.HASTA_DT OR S.HASTA_DT IS NULL))
        --Filtrado por actividad
        AND U.ACTIV_ID IN (1, 2)
        --Filtro CCAA

        AND E.VERSI_ID = (SELECT MAX(P2.VERSI_ID)
                          FROM DWE_VM_ELTREPOB P2
                          WHERE P2.UFTRG_ID = E.UFTRG_ID
                            AND P2.DESDE_DT = E.DESDE_DT)
        --Comprueba las fechas de los datos de población
        AND E.DESDE_DT >= cast ('2017-07-01' as date )
        AND E.DESDE_DT <= cast ('2017-07-31' as date )
		--and U.UNADM_ID ='UA1'
     --and G.UNGES_ID ='UG723' and R.TPREC_ID ='0113' --and L.ELMUN_ID ='33011'
	 --and (( U.UNADM_ID ='UA123' and L.ELMUN_ID=25006) or (  U.UNADM_ID ='UA1' and L.ELMUN_ID=14001))
GROUP BY E.DESDE_DT,
             U.UNADM_ID,
             U.ACTIV_ID,
             G.UNGES_ID,
             L.ELMUN_ID,
             F.UNFAC_ID,
             R.TPREC_ID,
             S.TPENT_ID,
             TF.TPGFA_ID,
             TP.PROCE_ID,E.POBIN_QT, E.POBLA_QT,--OPERADOR_ID_OP, OPERADOR_ID_OU,
			 OPERADOR_ID,
            V.PROVE_NM,POBDC_QT,PORCENTAJE_QT,coalesce(UTE_ID,0),PORCENTAJE_QT, -- PORCENTAJE DEL OPERADOR
             isnull(UTE_ID,0),
             PORCENTAJE_UTE_QT,OP.MEDIOSPP_SN