import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.nio.file.{Files, Paths}

object Aggregator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Aggregator")
      .master("spark://spark-master:7077")
      .getOrCreate()
      

    val inputPath = "/opt/airflow/input/"
    val intermediatePath = "/opt/airflow/intermediate/"
    val outputPath = "/opt/airflow/output/"
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    // Получаем текущую дату
    val currentDate = LocalDate.now()
    
    // Обрабатываем последние 7 дней
    for (i <- 1 until 8) {
      val date = currentDate.minusDays(i)
      val dateString = date.format(dateFormat)
      val filePath = s"$inputPath$dateString.csv"
      val intermediateFilePath = s"$intermediatePath$dateString.csv"

      // Проверяем, существует ли файл intermediate, и если его нет агрегируем и создаем
      if (!Files.exists(Paths.get(intermediateFilePath))) {
        // Читаем CSV файл
        val df = spark.read.option("header", "false").csv(filePath)
        val aggregatedDF = aggregateData(df)

        // Сохраняем агрегированные данные        
        aggregatedDF.write.option("header", "true").csv(intermediateFilePath)        
      }
    }

    // Агрегируем данные из промежуточных файлов
    val aggregatedOutputDF = aggregateIntermediateFiles(spark, currentDate, dateFormat, intermediatePath)
    
    // Сохраняем общий агрегированный файл
    aggregatedOutputDF.write.option("header", "true").csv(s"$outputPath${currentDate.format(dateFormat)}.csv")

    spark.stop()
  }

  def aggregateData(df: DataFrame): DataFrame = {
    df.withColumnRenamed("_c0", "email")
      .withColumnRenamed("_c1", "action")
      .groupBy("email")
      .agg(
        sum(when(col("action") === "CREATE", 1).otherwise(0)).alias("create_count"),
        sum(when(col("action") === "READ", 1).otherwise(0)).alias("read_count"),
        sum(when(col("action") === "UPDATE", 1).otherwise(0)).alias("update_count"),
        sum(when(col("action") === "DELETE", 1).otherwise(0)).alias("delete_count")
      )
  }

  def aggregateIntermediateFiles(spark: SparkSession, currentDate: LocalDate, dateFormat: DateTimeFormatter, intermediatePath: String): DataFrame = {
    val aggregatedDFs = (1 until 8).map { i =>
      val date = currentDate.minusDays(i)
      val dateString = date.format(dateFormat)
      val filePath = s"$intermediatePath$dateString.csv"
      if (Files.exists(Paths.get(filePath))) {
        spark.read.option("header", "true").csv(filePath)
      } else {
        spark.emptyDataFrame
      }
    }

    // Объединяем все DataFrame и агрегируем
    aggregatedDFs.reduce(_ union _).groupBy("email")
      .agg(
        sum("create_count").alias("create_count"),
        sum("read_count").alias("read_count"),
        sum("update_count").alias("update_count"),
        sum("delete_count").alias("delete_count")
      )
  }
}
