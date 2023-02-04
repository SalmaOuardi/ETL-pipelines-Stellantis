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
    app_name="[app00] Test_Read_Write_Data",
    driver_cores=1,
    driver_mem="4g",
    max_executors=8,
    executor_cores=4,
    executor_mem="4g"
)


# ### HDFS

# #### Read

# Data filepath
cycle = '202201'
flow_filepath = "/user/sd01865/crf0a/data/iCube/PROREV_%sIMP_AP.csv" %cycle
print(flow_filepath)


# Read data from HDFS
df_flow = spark_session.read.options(header='True', inferSchema='True', delimiter=';').csv(flow_filepath)
df_flow.printSchema()


df_flow.persist()

df_flow.count()


# ### Transformations

#change the data type of the Date column
df_flow = df_flow.withColumn("Date", F.to_date(F.col("Date").cast("string"),     'yyyyMM'))

#get the date value we will need to change the volume columns names
the_date = df_flow.collect()[0][4]
print(the_date)

#Rename the Valume colums with the date of the first of every month starting from the_date
df_flow = df_flow.withColumnRenamed("Volume 01",str(the_date))     .withColumnRenamed("Volume 02",str((the_date + datetime.timedelta(days=32)).replace(day=1)))     .withColumnRenamed("Volume 03",str((the_date + datetime.timedelta(days=32*2)).replace(day=1)))     .withColumnRenamed("Volume 04",str((the_date + datetime.timedelta(days=32*3)).replace(day=1)))     .withColumnRenamed("Volume 05",str((the_date + datetime.timedelta(days=32*4)).replace(day=1)))     .withColumnRenamed("Volume 06",str((the_date + datetime.timedelta(days=32*5)).replace(day=1)))     .withColumnRenamed("Volume 07",str((the_date + datetime.timedelta(days=32*6)).replace(day=1)))     .withColumnRenamed("Volume 08",str((the_date + datetime.timedelta(days=32*7)).replace(day=1)))     .withColumnRenamed("Volume 09",str((the_date + datetime.timedelta(days=32*8)).replace(day=1)))     .withColumnRenamed("Volume 10",str((the_date + datetime.timedelta(days=32*9)).replace(day=1)))     .withColumnRenamed("Volume 11",str((the_date + datetime.timedelta(days=32*10)).replace(day=1)))     .withColumnRenamed("Volume 12",str((the_date + datetime.timedelta(days=32*11)).replace(day=1)))     .withColumnRenamed("Date","Cycle")


pd_df=df_flow.toPandas()


#pivot the table
pivoted_df = pd_df.melt(id_vars = ['Marque', 'Pays','Libelle pays','Code version','Cycle'], value_vars = ['2022-01-01',
       '2022-02-01', '2022-03-01', '2022-04-01', '2022-05-01', '2022-06-01',
       '2022-07-01', '2022-08-01', '2022-09-01', '2022-10-01', '2022-11-01',
       '2022-12-01'], var_name = 'Date', value_name = 'Value')



#slice the Code version values to leave only the first 4 characters
pivoted_df['Code version']=pivoted_df['Code version'].str[:4]



#add the Inserted_date column
pivoted_df['INSERTED_DATE'] = datetime.date.today()

#add the Measure column
pivoted_df['Measure'] = 'DP IMP'


#Rename some columns to match the destination columns in Oracle Exadata
pivoted_df = pivoted_df.rename(columns={"Date":"MONTHYEAR"})
pivoted_df = pivoted_df.rename(columns={"Code version":"FAMILLE"})
pivoted_df = pivoted_df.rename(columns={"Libelle pays":"LIBELLE_PAYS"})
pivoted_df


#reordering the columns
cols = ('Marque', 'Pays', 'LIBELLE_PAYS', 'FAMILLE','MONTHYEAR','Measure','Value','Cycle','INSERTED_DATE')
pivoted_df = pivoted_df[list(cols)]
pivoted_df


from crf0a_app.infra.oracle_database import OracleDatabase


#Instantiate OracleDatabase object
oracle_db = OracleDatabase(dialect="jdbc", spark_session=spark_session)

#Convert the pandas dataframe to pyspark dataframe
sparkDF=spark_session.createDataFrame(pivoted_df)


# #### Write

# Write data to Oracle
oracle_db.write_df_to_oracle(
    sparkDF,
    "BRC_SD01865.DP_IMP_AP",
    mode="append"
)
