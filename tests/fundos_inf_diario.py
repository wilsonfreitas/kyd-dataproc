#!/usr/bin/env python
# coding: utf-8

import os
import shutil
import logging
import pandas as pd
import findspark
findspark.init()
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
import pyspark.sql.functions as F
import pyspark.sql
from pyspark.sql.types import DateType

logging.basicConfig(level=logging.INFO,
    format='========== %(asctime)s - %(levelname)s - %(message)s')

spark = pyspark.sql.SparkSession(sc)

# Enable Arrow-based columnar data transfers
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")

def process_file(data_fname):
    try:
        logging.info('%s', data_fname)
        fundos_id = spark.read.csv(data_fname,
                                    sep=';',
                                    encoding='latin1',
                                    dateFormat='yyyy-MM-dd',
                                    header=True,
                                    inferSchema=True)

        new_cols = [c.lower() for c in fundos_id.schema.names]
        fundos_id_proc = fundos_id.toDF(*new_cols)
        fundos_id_proc = fundos_id_proc.withColumn('dt_comptc', fundos_id_proc.dt_comptc.cast('date'))

        logging.info('converting to pandas')
        df = fundos_id_proc.toPandas()
        logging.info('pandas dataframe created')
        outputdata_fname = data_fname.replace('.csv', '.parquet')
        df.to_parquet(outputdata_fname, compression='gzip', engine='pyarrow')
        logging.info('parquet saved %s', outputdata_fname)
    except Exception as ex:
        logging.error(data_fname)
        logging.error(str(ex))

output_data = 'data'
data_dir = f'{output_data}/inf_diario_fi'
files = [f'{data_dir}/{f}' for f in os.listdir(data_dir) if f.endswith('.csv')]
for data_fname in files:
    process_file(data_fname)

# output_temp = 'outputtemp'
# fundos_id_proc.write.parquet(output_temp, mode='overwrite', compression='gzip')
# logging.info('parquet saved at %s', output_temp)
# get_ipython().system('ls data')
# fundos_id_proc.show(10)
# fnames = [f'{output_temp}/{f}' for f in os.listdir(output_temp) if f.endswith('.parquet')]
# df = pd.read_parquet(fnames[0])
# for fname in fnames[1:]:
#     df_x = pd.read_parquet(fname)
#     df = df.append(df_x)
# clean
# shutil.rmtree(output_temp)
