from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import os
from functools import reduce

def main():
    spark = SparkSession.builder \
        .appName("CSV Aggregator") \
        .master("local[*]") \
        .getOrCreate()

    input_path = "input/"
    intermediate_path = "intermediate/"
    output_path = "output/"
    date_format = "%Y-%m-%d"
    # Получаем текущую дату
    current_date = datetime.now()

    # Обрабатываем последние 7 дней
    for i in range(1, 8):
        date = current_date - timedelta(days=i)
        date_string = date.strftime(date_format)
        file_path = os.path.join(input_path, f"{date_string}.csv")
        intermediate_file_path = os.path.join(intermediate_path, f"{date_string}.csv")

        # Проверяем, существует ли файл intermediate, и если его нет, агрегируем и создаем
        if not os.path.exists(intermediate_file_path):
            # Читаем CSV файл
            df = spark.read.option("header", "false").csv(file_path)
            aggregated_df = aggregate_data(df)

            # Сохраняем агрегированные данные        
            aggregated_df.write.option("header", "true").csv(intermediate_file_path)

    # Агрегируем данные из промежуточных файлов
    aggregated_output_df = aggregate_intermediate_files(spark, current_date, date_format, intermediate_path)

    # Сохраняем общий агрегированный файл
    aggregated_output_df.write.option("header", "true").csv(os.path.join(output_path, f"{current_date.strftime(date_format)}.csv"))

    spark.stop()

def aggregate_data(df):
    return (df.withColumnRenamed("_c0", "email")
              .withColumnRenamed("_c1", "action")
              .groupBy("email")
              .agg(
                  F.sum(F.when(F.col("action") == "CREATE", 1).otherwise(0)).alias("create_count"),
                  F.sum(F.when(F.col("action") == "READ", 1).otherwise(0)).alias("read_count"),
                  F.sum(F.when(F.col("action") == "UPDATE", 1).otherwise(0)).alias("update_count"),
                  F.sum(F.when(F.col("action") == "DELETE", 1).otherwise(0)).alias("delete_count")
              ))

def aggregate_intermediate_files(spark, current_date, date_format, intermediate_path):
    aggregated_dfs = []
    for i in range(1, 8):
        date = current_date - timedelta(days=i)
        date_string = date.strftime(date_format)
        file_path = os.path.join(intermediate_path, f"{date_string}.csv")
        if os.path.exists(file_path):
            df = spark.read.option("header", "true").csv(file_path)
            aggregated_dfs.append(df)

    # Объединяем все DataFrame и агрегируем
    if aggregated_dfs:
        return (reduce(lambda df1, df2: df1.union(df2), aggregated_dfs)  # Объединяем все DataFrame
                .groupBy("email")
                .agg(
                    F.sum("create_count").alias("create_count"),
                    F.sum("read_count").alias("read_count"),
                    F.sum("update_count").alias("update_count"),
                    F.sum("delete_count").alias("delete_count")
                ))
    else:
        return spark.createDataFrame([], schema="email STRING, create_count INT, read_count INT, update_count INT, delete_count INT")

if __name__ == "__main__":
    main()
