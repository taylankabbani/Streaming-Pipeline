import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, max, rank, row_number, abs, date_sub, date_add, col, lit
import datetime
import logging

scala_version = '2.12'
spark_version = '3.1.2'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]
# Creating a logger object
logging.getLogger().addHandler(logging.StreamHandler())
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class CorrelationReport:
    """A job spark that extract data from source (parquet), 
    transform it by finding the the top three metrics correlated to the increase in production output"""

    def __init__(self, database_path: str):
        self.spark = self._init_spark()
        self.database_path = database_path
        self.tables = self._get_database_tables()
        self.top_3_data = {}
        self.out_date = {}
        self.folder_name = os.path.join("/home/ETL_out",time.strftime("%Y%m%d-%H%M%S"))

    @staticmethod
    def _init_spark():
        try:
            spark = SparkSession.builder \
                .appName("CorrelationReport") \
                .master("spark://192.168.0.4:7077") \
                .config("spark.driver.host", "192.168.0.4")\
                .config("spark.jars.packages", ",".join(packages))\
                .getOrCreate()

            _logger.info(
                f"------------ Initiated Spark Session Successfully!! ------------")
            return spark
        except Exception as e:
            _logger.error(f"Failed to initiate spark session because of:", e)
    def _create_folder(self):
        if not os.path.exists(self.folder_name):
            os.makedirs(self.folder_name)
            
    def _load_data(self, path: str):
        df = self.spark.read.parquet(path)
        return df

    def _get_database_tables(self):
        tables = {}
        assert os.path.exists(
            self.database_path), f"Database is not found at: {self.database_path} "
        for table in os.listdir(self.database_path):
            table_path = os.path.join(self.database_path, table)
            try:
                df = self._load_data(table_path)
                tables[table] = df
                _logger.info(
                    f"------------ Successfully loaded {table} table  ------------")
            except Exception as e:
                _logger.error(
                    f"Failed to load {table} data table because of: {e}")
        return tables

    def _add_product_per_time_interval(self, df):
        # create a window func
        w = Window.partitionBy("product").orderBy("time")
        # Creating product count per time interval for each product
        production_interval = df['production'] - \
            lag(df["production"], 1).over(w)
        # order diff in descending order
        df = df.withColumn("diff", production_interval)
        return df

    def _get_top_3(self, df):
        # create a window func
        w = Window.partitionBy("product").orderBy(df["diff"].desc())
        # Use the window function to assign row numbers to each record within each group
        df = df.withColumn("rank", row_number().over(w))
        # Filter out the records with a rank greater than three
        df = df.filter(df["rank"] <= 3)
        return df

    # Define a function to find the closest timestamp to a given timestamp
    def _closest_timestamp(self, timestamp, df):
        # Define the number of days to filter
        interval = 1
        # Compute the start and end dates of the interval
        start_date = date_sub(lit(timestamp), interval)
        end_date = date_add(lit(timestamp), interval)
        # Filter the DataFrame based on the interval
        filtered_df = df.filter(
            (col("time") >= start_date) & (col("time") <= end_date))
        return filtered_df.orderBy(abs(timestamp - df["time"]).asc()).limit(1)

    def get_schema(self):
        for table in self.tables.values():
            table.printSchema()

    def _get_metrics(self, df, ls):
        # Use list comprehension to filter the DataFrame based on the closest timestamp to each element in the list
        filtered_df = self.spark.createDataFrame(
            [self._closest_timestamp(t, df).first() for t in ls], ["id", "value", "time"])
        return filtered_df

    def run(self):
        self._create_folder()
        rdd_1 = self._add_product_per_time_interval(self.tables["workorder"])
        rdd_2 = self._get_top_3(rdd_1)
        top_3 = rdd_2.collect()
        for item in top_3:
            if item.product not in self.top_3_data.keys():
                self.top_3_data[item.product] = {"time": [], "diff": []}
            self.top_3_data[item.product]['time'].append(item.time)
            self.top_3_data[item.product]['diff'].append(item.diff)
        # todo
        ############## This not cool i know :(, this is the pipeline bottleneck ######
        df = self.tables["metrics"]
        for id in self.top_3_data.keys():
            df_id = df.filter(df["id"] == id)
            out = self._get_metrics(df_id, self.top_3_data[id]['time'])
            # Write the DataFrame to a CSV file
            try:
                out.toPandas().to_csv(f'{self.folder_name}/{id}.csv')
                print(f"------------ Saved report of product {self.folder_name}/{id}\n ------------") 
            except Exception as e:
                print(f"Failed to write output CSV file due to: {e}")
                raise e

if __name__ == "__main__":
    ETL = CorrelationReport(
        database_path="/home/database")
    ETL.run()
