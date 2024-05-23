# Databricks notebook source
# MAGIC %run "./reader_factory" 

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./Transform"

# COMMAND ----------

# MAGIC %run "./Loader"

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("apple_store_analysis").getOrCreate()

input_df=spark.read.format("csv").option("header","True").load("dbfs:/FileStore/tables/Transaction_Updated.csv")
input_df.show()

# COMMAND ----------

class FirstWorkflow:
    """ETL Pipeline to generate the data for all customers who have brought Airpods after buying iPhone"""
    def __init__(self):
        pass

    def runner(self):
        #Step 1:Extract all required data from different sources
        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #Step 2:Implement the Transformation Logic
        #Customer who have brought airpods after buying iphone
        firstTransformedDF=AirpodsAfterIphoneTransformer().transform(inputDFs)

        #Step 3:Load all required data to different sink
        AirpodsAfterIphoneLoader(firstTransformedDF).sink()

# COMMAND ----------

class SecondWorkflow:
    """ETL Pipeline to generate the data for all customers who have brought only Airpods & iPhone"""
    def __init__(self):
        pass

    def runner(self):
        #Step 1:Extract all required data from different sources
        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #Step 2:Implement the Transformation Logic
        #Customer who have brought only airpods & iphone
        onlyAirpodsAndIphoneDF=OnlyAirpodsandIphone().transform(inputDFs)

        #Step 3:Load all required data to different sink
        OnlyAirpodsAndIphoneLoader(onlyAirpodsAndIphoneDF).sink()

# COMMAND ----------

class WorkFlowRunner:

    def __init__(self,name):
        self.name=name
    
    def runner(self):
        if self.name == "firstWorkFlow":
            return FirstWorkflow().runner()
        elif self.name == "secondWorkFlow":
            return SecondWorkflow().runner()
        else:
            raise ValueError(f"Not Implemented for {self.name}")
        
name="firstWorkFlow"

workflowrunner=WorkFlowRunner(name).runner()

