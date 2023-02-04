/* Query for Retails ACTUALS */

SELECT
    a1.libelle_pp_5   program_country,
    a1.famille_0      family,
    CAST(a1.date_photo_2 AS DATE) monthyear,
    'Retails' measure,
    SUM(a1.tentr_m_1) value,
    to_char(sysdate)   inserted_date,
    '{cycle}' cycle
FROM
    (
        SELECT
            a3.famille      famille_0,
            a3.tentr_m       tentr_m_1,
            CAST(a3.date_photo AS DATE)   date_photo_2,
            a3.qi_filial    qi_filial,
            a2.qi_filiale   qi_filiale,
            a2.libelle_pp   libelle_pp_5
        FROM
            brc06_bds00.rbvqtdne   a3,
            brc06_bds00.rbvqtpco   a2
        WHERE
            a2.qi_filiale = a3.qi_filial
            AND a3.date_photo >= TO_DATE('{firstday_lastMonth}', 'dd/mm/yyyy')
            AND a3.date_photo <= TO_DATE('{lastday_lastMonth2}', 'dd/mm/yyyy')
    ) a1
WHERE
    ( a1.libelle_pp_5 = 'AFRIQUE' )
    OR ( a1.libelle_pp_5 = 'ALGERIE' )
    OR ( a1.libelle_pp_5 = 'ZONE ALGERIE' )
    OR ( a1.libelle_pp_5 = 'EGYPTE' )
    OR ( a1.libelle_pp_5 = 'MASHREQ' )
    OR ( a1.libelle_pp_5 = 'MAURICE AC' )
    OR ( a1.libelle_pp_5 = 'MAURICE OV' )
    OR ( a1.libelle_pp_5 = 'MAURICE AP' )
    OR ( a1.libelle_pp_5 = 'MASHREQ ZONE OV' )
    OR ( a1.libelle_pp_5 = 'MAROC' )
    OR ( a1.libelle_pp_5 = 'NIGERIA' )
    OR ( a1.libelle_pp_5 = 'ARABIE' )
    OR ( a1.libelle_pp_5 = 'AFRIQUE DU SUD' )
    OR ( a1.libelle_pp_5 = 'ZONE TUNISIE' )
    OR ( a1.libelle_pp_5 = 'TURQUIE' )
    OR ( a1.libelle_pp_5 = 'TURQUIE DS' )
GROUP BY
    a1.libelle_pp_5,
    a1.famille_0,
    CAST(a1.date_photo_2 AS DATE) ,
    'retails',
    to_char(sysdate),
    '{cycle}'
HAVING
    SUM(a1.tentr_m_1) IS NOT NULL
ORDER BY
    monthyear DESC,
    value DESC
FETCH FIRST 10 ROWS ONLY