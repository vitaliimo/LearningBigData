from pyspark.sql import DataFrame, SparkSession

APP_NAME = "ReadWrite"
CSV_INPUT_LOCATION = "data/csv/actors.csv"
PARQUET_OUTPUT_LOCATION = "data/parquet"
AVRO_OUTPUT_LOCATION = "data/avro"
ORC_OUTPUT_LOCATION = "data/orc"

spark: SparkSession = None


def create_spark_session(app_name: str) -> SparkSession:
    print("Creating Spark Session")
    return SparkSession \
        .builder \
        .master("local") \
        .appName(app_name) \
        .getOrCreate()


def read_csv(file_location: str) -> DataFrame:
    print(f"Reading CSV file from '{file_location}'")
    return spark \
        .read \
        .option("header", True) \
        .csv(file_location)


def info(data_frame: DataFrame):
    print("\nSchema: ")
    data_frame.printSchema()
    print("\nData preview: ")
    data_frame.show()


def write_parquet(data_frame: DataFrame, location: str):
    print(f"Write Dataset in Parquet format to location: '{location}'")
    data_frame.write.parquet(location, "overwrite")


def write_avro(data_frame: DataFrame, location: str):
    print(f"Write Dataset in Avro format to location: '{location}'")
    data_frame.write.format("avro").save(location)


def write_orc(data_frame: DataFrame, location: str):
    print(f"Write Dataset in ORC format to location: '{location}'")
    data_frame.write.orc(location, "overwrite")


def stop():
    print("Stopping Spark")
    spark.stop()


def run():
    global spark
    spark = create_spark_session(APP_NAME)
    data_frame = read_csv(CSV_INPUT_LOCATION)
    info(data_frame)
    write_parquet(data_frame, PARQUET_OUTPUT_LOCATION)
    write_orc(data_frame, ORC_OUTPUT_LOCATION)
    stop()
