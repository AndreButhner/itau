from datetime import date, datetime, timedelta
import os

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *



spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-alert') \
  .getOrCreate()

bucket = "gcp-bucket"
spark.conf.set('temporaryGcsBucket', bucket)

current_safra = (date.today() - timedelta(1)).strftime("%Y-%m")

#Reading data from table
tblTransaction = spark.read.format('BigQuery') \
.option('table', 'gcp-project.dataset_transaction.tbl_fact_transaction') \
.option('filter', "safra" == current_safra)
.load()

#Creating a temporary table
tblTransaction.createOrReplaceTempView('tbl_fact_transaction')

#Selecting columns to be used in alert rules
dsAlert = tblTransaction.select('num_document', 'category', 'type_transaction', 'val_transaction', 'safra') \
.filter(tblTransaction.type_transaction == 'CASH')

#Applying rules
dsAlert = dsAlert.withColumn('ds_alert', when(dsAlert.category == 'UNICLASS' && sum(dsAlert.val_transaction) > 5000, 'warning') \
										.when(dsAlert.category == 'UNICLASS' && sum(dsAlert.val_transaction) > 10000, 'critical') \
											.when(dsAlert.category == 'PERSONALITE' && sum(dsAlert.val_transaction) > 10000, 'warning') \
											.when(dsAlert.category == 'PERSONALITE' && sum(dsAlert.val_transaction) > 20000, 'critical') \
												.when(dsAlert.category == 'PRIVATE' && sum(dsAlert.val_transaction) > 20000, 'warning') \
												.when(dsAlert.category == 'PRIVATE' && sum(dsAlert.val_transaction) > 30000, 'critical'))

#Saving table
dsAlert.write.format('bigquery') \
  .option('table', 'gcp-project.dataset_transaction.tbl_alert') \
  .save()