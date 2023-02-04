#!/usr/bin/env python
# coding: utf-8

# ### Imports

get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')
import os
import datetime
import pandas as pd
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 100)
from pyspark.sql import functions as F

from crf0a_app.configuration import spark_config
from crf0a_app.utils import system


# ### Spark session

spark_context, spark_session = spark_config.get_spark(
    app_name="[crf0A] Write to Exadata",
    driver_cores=1,
    driver_mem="4g",
    max_executors=8,
    executor_cores=4,
    executor_mem="4g"
)


# ### Exadata

# #### Instantiate OracleDatabase object

from crf0a_app.infra.oracle_database import OracleDatabase

oracle_db = OracleDatabase(dialect="jdbc", spark_session=spark_session)

from datetime import date


# #### Read

today = str(date.today())
cycle = today[0:4]+today[5:7]


today = datetime.date.today()
first = today.replace(day=1)
lastday_lastMonth1 = (first - datetime.timedelta(days=1))
lastday_lastMonth2 = (first - datetime.timedelta(days=1)).strftime("%d/%m/%Y")
print(lastday_lastMonth2)


today = datetime.date.today()
first = today.replace(day=1)
firstday_lastMonth = (first - datetime.timedelta(days=lastday_lastMonth1.day)).strftime("%d/%m/%Y")
print(firstday_lastMonth)


# SQL query
PROD_DEM = f"""
(SELECT
    a1.libelle_pp_5   program_country,
    a1.famille_0      family,
    CAST(a1.date_photo_2 AS DATE) monthyear,
    'Wholesales' measure,
    SUM(a1.tfac_m_1) value,
    to_char(sysdate)   inserted_date,
    '{cycle}' cycle
FROM
    (
        SELECT
            a3.famille      famille_0,
            a3.tfac_m       tfac_m_1,
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
    a1.date_photo_2 ,
    'Wholesales',
    to_char(sysdate),
    '{cycle}'
HAVING
    SUM(a1.tfac_m_1) IS NOT NULL
ORDER BY
    monthyear DESC,
    value DESC
FETCH FIRST 10 ROWS ONLY
)
"""


# Read data from Oracle
wholesales = oracle_db.read_df_from_query(PROD_DEM, fetchsize=20000)


wholesales.persist()


wholesales.limit(5).toPandas()


wholesales.count()



# Write data to Oracle
oracle_db.write_df_to_oracle(
    wholesales,
    "BRC_SD01865.WS_ACT",
    mode="overwrite"
)
