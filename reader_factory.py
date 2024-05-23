# Databricks notebook source
class DataSource:
    def __init__(self,path):
        self.path=path
    """Abstract Class"""
    def get_data_frame(self, path):
        self.path=path
    
    def get_data_frame(self):
        """
        Abstract Method, Functions will be defined in sub classes
        """
        raise ValueError("Not implemented")
    
class CSVDataSource(DataSource):
    """sub-class can inherit all functions of parent class"""
    def get_data_frame(self):
        return (
            spark.read.format("csv")
            .option("header", "True")
            .load(self.path)
        )

class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return (
            spark.read.format("parquet")
            .option("header", "True")
            .load(self.path)
        )

class DeltaDataSource(DataSource):
    def get_data_frame(self):
        table_name=self.path
        return (
            spark.read.
            table(table_name)
        )

"""function to call the methods"""
def get_data_source(data_type, file_path):
    if data_type == "csv":
        return CSVDataSource(file_path)
    elif data_type == "parquet":
        return ParquetDataSource(file_path)
    elif data_type == "delta":
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f"Not implemented for data_type: {data_type}") 