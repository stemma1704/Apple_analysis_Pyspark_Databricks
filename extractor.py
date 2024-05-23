# Databricks notebook source
# MAGIC %run "./Transform"

# COMMAND ----------

# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """ Abstract class"""
    def __init__(self):
        pass
    def extractor(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):

    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        transaction_input_df = get_data_source(
            data_type = "csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()

        transaction_input_df.orderBy("customer_id","transaction_date").show()

        customer_input_df = get_data_source(
            data_type = "delta",
            file_path="default.customer_delta_table"
        ).get_data_frame()

        
        
        inputDFs = {
            "transaction_input_df": transaction_input_df,
            "customer_input_df": customer_input_df
        }

        return inputDFs

    