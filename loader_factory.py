# Databricks notebook source
class DataSink:
    def __init__(self, df, path, method, params=None):
        self.path = path
        self.df = df
        self.method = method
        self.params = params

    def load_data_frame(self):
        raise ValueError("Not implemented")

class LoadToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)

class LoadToDBFSWithPartition(DataSink):
    def load_data_frame(self):
        partitionByColumns = self.params.get("partitionByColumns")
        if partitionByColumns:
            self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)
        else:
            raise ValueError("Partition columns not provided for DBFS with partition sink.")

class LoadToDeltaTable(DataSink):
    def load_data_frame(self):
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)

def get_sink_source(sink_type, df, path, method, params=None):
    if sink_type == "dbfs":
        return LoadToDBFS(df, path, method, params)
    elif sink_type == "dbfs_with_partition":
        return LoadToDBFSWithPartition(df, path, method, params)
    elif sink_type == "delta":
        return LoadToDeltaTable(df, path, method, params)
    else:
        raise ValueError(f"Not implemented for data type: {sink_type}")
