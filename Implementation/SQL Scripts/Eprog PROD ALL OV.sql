/* Query for OV / PROD ALL */
SELECT
    a1.grep_libelle_pp_22    program_country,
    a1.fa1_code_famille_12   family,
    '01' || '/' || a1.pe1_numero_15 || '/' || a1.pe1_annee_14 monthyear,
    'PROD ALL' Measure,
    SUM(a1.vpn_volume_alloue_5) VALEUR,
    to_char(sysdate) inserted_date,
    a1.ca1_cycle_11 cycle

FROM
    (
        SELECT
            a3.vpn_id_pays_unitaire_0           vpn_id_pays_unitaire,
            a3.vpn_code_marque_1                vpn_code_marque_1,
            a3.vpn_num_cycle_2                  vpn_num_cycle,
            a3.vpn_annee_cycle_3                vpn_annee_cycle,
            a3.vpn_periode_4                    vpn_periode,
            a3.vpn_volume_alloue_5              vpn_volume_alloue_5,
            a3.vpn_est_bascule_6                vpn_est_bascule_6,
            a3.ca1_code_marque_7                ca1_code_marque,
            a3.ca1_annee_cycle_8                ca1_annee_cycle,
            a3.ca1_numero_cycle_9               ca1_numero_cycle,
            a3.ca1_code_zone_production_10      ca1_code_zone_production_10,
            a3.ca1_cycle_11                     ca1_cycle_11,
            a3.fa1_code_famille_12              fa1_code_famille_12,
            a3.fa1_code_marque_13               fa1_code_marque,
            a3.pe1_annee_14                     pe1_annee_14,
            a3.pe1_numero_15                    pe1_numero_15,
            a3.pe1_code_zone_production_16      pe1_code_zone_production_16,
            a3.pe1_periode_17                   pe1_periode,
            a3.pu1_id_pays_unitaire_18          pu1_id_pays_unitaire,
            a3.pu1_code_psv_pays_programme_19   pu1_code_psv_pays_programme,
            a2.grep_libelle_groupe_reporting    grep_libelle_groupe_reporting_20,
            a2.grep_code_psv_pays_programme     grep_code_psv_pays_programme,
            a2.grep_libelle_pp                  grep_libelle_pp_22
        FROM
            (
                SELECT
                    a5.vpn_id_pays_unitaire_0        vpn_id_pays_unitaire_0,
                    a5.vpn_code_marque_1             vpn_code_marque_1,
                    a5.vpn_num_cycle_2               vpn_num_cycle_2,
                    a5.vpn_annee_cycle_3             vpn_annee_cycle_3,
                    a5.vpn_periode_4                 vpn_periode_4,
                    a5.vpn_volume_alloue_5           vpn_volume_alloue_5,
                    a5.vpn_est_bascule_6             vpn_est_bascule_6,
                    a5.ca1_code_marque_7             ca1_code_marque_7,
                    a5.ca1_annee_cycle_8             ca1_annee_cycle_8,
                    a5.ca1_numero_cycle_9            ca1_numero_cycle_9,
                    a5.ca1_code_zone_production_10   ca1_code_zone_production_10,
                    a5.ca1_cycle_11                  ca1_cycle_11,
                    a5.fa1_code_famille_12           fa1_code_famille_12,
                    a5.fa1_code_marque_13            fa1_code_marque_13,
                    a5.pe1_annee_14                  pe1_annee_14,
                    a5.pe1_numero_15                 pe1_numero_15,
                    a5.pe1_code_zone_production_16   pe1_code_zone_production_16,
                    a5.pe1_periode_17                pe1_periode_17,
                    a4.pu1_id_pays_unitaire          pu1_id_pays_unitaire_18,
                    a4.pu1_code_psv_pays_programme   pu1_code_psv_pays_programme_19
                FROM
                    (
                        SELECT
                            a7.vpn_id_pays_unitaire_0        vpn_id_pays_unitaire_0,
                            a7.vpn_code_marque_1             vpn_code_marque_1,
                            a7.vpn_num_cycle_2               vpn_num_cycle_2,
                            a7.vpn_annee_cycle_3             vpn_annee_cycle_3,
                            a7.vpn_periode_4                 vpn_periode_4,
                            a7.vpn_volume_alloue_5           vpn_volume_alloue_5,
                            a7.vpn_est_bascule_6             vpn_est_bascule_6,
                            a7.ca1_code_marque_7             ca1_code_marque_7,
                            a7.ca1_annee_cycle_8             ca1_annee_cycle_8,
                            a7.ca1_numero_cycle_9            ca1_numero_cycle_9,
                            a7.ca1_code_zone_production_10   ca1_code_zone_production_10,
                            a7.ca1_cycle_11                  ca1_cycle_11,
                            a7.fa1_code_famille_12           fa1_code_famille_12,
                            a7.fa1_code_marque_13            fa1_code_marque_13,
                            a6.pe1_annee                     pe1_annee_14,
                            a6.pe1_numero                    pe1_numero_15,
                            a6.pe1_code_zone_production      pe1_code_zone_production_16,
                            a6.pe1_periode                   pe1_periode_17
                        FROM
                            (
                                SELECT
                                    a9.vpn_id_pays_unitaire_0        vpn_id_pays_unitaire_0,
                                    a9.vpn_code_marque_1             vpn_code_marque_1,
                                    a9.vpn_num_cycle_2               vpn_num_cycle_2,
                                    a9.vpn_annee_cycle_3             vpn_annee_cycle_3,
                                    a9.vpn_periode_4                 vpn_periode_4,
                                    a9.vpn_volume_alloue_5           vpn_volume_alloue_5,
                                    a9.vpn_est_bascule_6             vpn_est_bascule_6,
                                    a9.ca1_code_marque_7             ca1_code_marque_7,
                                    a9.ca1_annee_cycle_8             ca1_annee_cycle_8,
                                    a9.ca1_numero_cycle_9            ca1_numero_cycle_9,
                                    a9.ca1_code_zone_production_10   ca1_code_zone_production_10,
                                    a9.ca1_cycle_11                  ca1_cycle_11,
                                    a8.fa1_code_famille              fa1_code_famille_12,
                                    a8.fa1_code_marque               fa1_code_marque_13
                                FROM
                                    (
                                        SELECT
                                            a11.vpn_id_pays_unitaire       vpn_id_pays_unitaire_0,
                                            a11.vpn_code_marque            vpn_code_marque_1,
                                            a11.vpn_num_cycle              vpn_num_cycle_2,
                                            a11.vpn_annee_cycle            vpn_annee_cycle_3,
                                            a11.vpn_periode                vpn_periode_4,
                                            a11.vpn_volume_alloue          vpn_volume_alloue_5,
                                            a11.vpn_est_bascule            vpn_est_bascule_6,
                                            a10.ca1_code_marque            ca1_code_marque_7,
                                            a10.ca1_annee_cycle            ca1_annee_cycle_8,
                                            a10.ca1_numero_cycle           ca1_numero_cycle_9,
                                            a10.ca1_code_zone_production   ca1_code_zone_production_10,
                                            a10.ca1_cycle                  ca1_cycle_11
                                        FROM
                                            brc06_bpg00.rbvqtvpn   a11,
                                            brc06_bpg00.rbvqtca1   a10
                                        WHERE
                                            a10.ca1_code_marque = a11.vpn_code_marque
                                            AND a10.ca1_annee_cycle = a11.vpn_annee_cycle
                                            AND a10.ca1_numero_cycle = a11.vpn_num_cycle
                                    ) a9,
                                    brc06_bpg00.rbvqtfa1 a8
                                WHERE
                                    a9.vpn_code_marque_1 = a8.fa1_code_marque
                            ) a7,
                            brc06_bpg00.rbvqtpe1 a6
                        WHERE
                            a7.vpn_periode_4 = a6.pe1_periode
                    ) a5,
                    brc06_bpg00.rbvqtpu1 a4
                WHERE
                    a5.vpn_id_pays_unitaire_0 = a4.pu1_id_pays_unitaire
            ) a3,
            brc06_bpg00.rbvqtgrep a2
        WHERE
            a3.pu1_code_psv_pays_programme_19 = a2.grep_code_psv_pays_programme
    ) a1
WHERE
    a1.ca1_cycle_11 = 202206
    AND a1.ca1_code_zone_production_10 LIKE 'PSA'
    AND a1.vpn_est_bascule_6 LIKE 'N'
    AND a1.pe1_code_zone_production_16 LIKE 'PSA'
    AND a1.vpn_code_marque_1 LIKE 'OV'
    AND a1.grep_libelle_groupe_reporting_20 LIKE '500-DMOA'
GROUP BY
    a1.fa1_code_famille_12, 
    a1.pe1_annee_14, 
    '01' || '/' || a1.pe1_numero_15 || '/' || a1.pe1_annee_14, 
    'PROD ALL', 
    a1.pe1_numero_15, 
    a1.ca1_cycle_11, 
    a1.grep_libelle_pp_22, 
    to_char(sysdate) 
HAVING
    SUM(a1.vpn_volume_alloue_5) IS NOT NULL
ORDER BY
    "VALEUR" DESC
FETCH NEXT 10 ROWS ONLY;