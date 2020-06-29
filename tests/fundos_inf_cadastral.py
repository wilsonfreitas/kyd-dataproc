#!/usr/bin/env python
# coding: utf-8

import logging
import pandas as pd

import findspark
findspark.init()
from pyspark import SparkContext
sc = SparkContext("local", "first app")
import pyspark.sql.functions as F
import pyspark.sql
from pyspark.sql.types import DateType

spark = pyspark.sql.SparkSession(sc)

output_data = 'data'
data_dir = f'{output_data}/inf_cadastral_fi'
data_fname = f'{data_dir}/inf_cadastral_fi_20200529.csv'

fundos_cad = spark.read.csv(data_fname,
                            sep=';',
                            encoding='latin1',
                            dateFormat='yyyy-MM-dd',
                            header=True,
                            inferSchema=True)

new_cols = [c.lower() for c in fundos_cad.schema.names]
fundos_cad_proc = fundos_cad.toDF(*new_cols)
fundos_cad_proc = fundos_cad_proc.withColumn('dt_reg', fundos_cad_proc.dt_reg.cast('date'))                                 .withColumn('dt_const', fundos_cad_proc.dt_const.cast('date'))                                 .withColumn('dt_cancel', fundos_cad_proc.dt_cancel.cast('date'))                                 .withColumn('dt_ini_sit', fundos_cad_proc.dt_ini_sit.cast('date'))                                 .withColumn('dt_ini_ativ', fundos_cad_proc.dt_ini_ativ.cast('date'))                                 .withColumn('dt_ini_exerc', fundos_cad_proc.dt_ini_exerc.cast('date'))                                 .withColumn('dt_fim_exerc', fundos_cad_proc.dt_fim_exerc.cast('date'))                                 .withColumn('dt_ini_classe', fundos_cad_proc.dt_ini_classe.cast('date'))                                 .withColumn('dt_patrim_liq', fundos_cad_proc.dt_patrim_liq.cast('date'))
fundos_cad_proc = fundos_cad_proc.withColumn('fundo_cotas',
                           F.when(fundos_cad_proc.fundo_cotas == 'S', True)\
                            .when(fundos_cad_proc.fundo_cotas == 'N', False)\
                            .otherwise(None))
fundos_cad_proc = fundos_cad_proc.withColumn('fundo_exclusivo',
                           F.when(fundos_cad_proc.fundo_exclusivo == 'S', True)\
                            .when(fundos_cad_proc.fundo_exclusivo == 'N', False)\
                            .otherwise(None))
fundos_cad_proc = fundos_cad_proc.withColumn('trib_lprazo',
                           F.when(fundos_cad_proc.trib_lprazo == 'S', True)\
                            .when(fundos_cad_proc.trib_lprazo == 'N', False)\
                            .otherwise(None))
fundos_cad_proc = fundos_cad_proc.withColumn('invest_qualif',
                           F.when(fundos_cad_proc.invest_qualif == 'S', True)\
                            .when(fundos_cad_proc.invest_qualif == 'N', False)\
                            .otherwise(None))

logging.info('converting to pandas')
df = fundos_cad_proc.toPandas()
logging.info('pandas dataframe created')
outputdata_fname = data_fname.replace('.csv', '.parquet')
df.to_parquet(outputdata_fname, compression='gzip', engine='pyarrow')
logging.info('parquet saved %s', outputdata_fname)



