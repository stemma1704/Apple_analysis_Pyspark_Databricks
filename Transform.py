# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col,broadcast,collect_set,size,array_contains

# COMMAND ----------

class Transformer:
    def __init__(self):
        pass
    def transformer(self,inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self,inputDFs):

        """Customer who have brought airpods after buying iphone"""

        transaction_input_df=inputDFs.get("transaction_input_df")

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transaction_input_df.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        filteredDF = transformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        customer_input_df=inputDFs.get("customer_input_df")

        JoinDF=filteredDF.join(
            broadcast(customer_input_df),"customer_id")
        
        print("Query 1:Customers who brought airpods after iphone: ")
        JoinDF.show()

        return JoinDF.select("customer_id","customer_name","location","next_product_name")
        

class OnlyAirpodsandIphone(Transformer):
    """Customer who have brought iPhone and Airpods only"""
    def transform(self,inputDFs):
        transaction_input_df=inputDFs.get("transaction_input_df")

        groupedDF=transaction_input_df.groupBy("customer_id").agg(collect_set("product_name").alias("products"))

        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) & 
            (size(col("products"))==2)
        )
        
        customer_input_df=inputDFs.get("customer_input_df")
        JoinDF_2=customer_input_df.join(broadcast(filteredDF),"customer_id")

        print("Query 2:Customers who brought iphone and airpods:")
        JoinDF_2.show()

        return JoinDF_2.select("customer_id","customer_name","location")