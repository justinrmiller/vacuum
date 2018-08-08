package com.justinrmiller.vacuum

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  def processTable(
    spark: SparkSession,
    extractor: MetadataExtractor,
    tableName: String,
    filteredColumnMetadata: Array[ColumnMetadata],
    saveMode: SaveMode = SaveMode.Append
  ): Future[Unit] = Future {
    println(s"Trying for $tableName to have the following structtype")

    val df =
      spark
        .read
        //.jdbc(extractor.url, tableName, new Properties)
        .format("jdbc")
        .options(extractor.options + ("dbtable" -> tableName))
        .load

    df.printSchema()

    val transformedColumns = filteredColumnMetadata.map { columnMetadata: ColumnMetadata =>
      // automatically promote integer to long value and store
      val transformedColumn = if (columnMetadata.data_type == "integer") {
        col(columnMetadata.column_name).cast("long")
      } else {
        col(columnMetadata.column_name)
      }

      transformedColumn
    }

    if (transformedColumns.isEmpty) {
      println(s"All columns for $tableName were filtered out. Skipping saving table.")
    } else {
      println(s"Saving off transformed table $tableName")
      val transformedDF = df.select(transformedColumns.toSeq: _*)

      val transformedNullableDF = spark.sqlContext.createDataFrame(
        transformedDF.rdd,
        StructType(transformedDF.schema.map {
          case StructField( c, t, _, m) ⇒ StructField( c, t, nullable = true, m)    // make all columns nullable
        })
      )

      transformedNullableDF.printSchema()

      transformedNullableDF
        .write
        .mode(saveMode = saveMode)
        .parquet(s"/Users/justin/db/$tableName")
    }
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Main").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._

    val databaseName = "justin"

    //todo perhaps take the spark session as an implicit parameter?
    val extractor = new PostgresMetadataExtractor(session = spark, databaseName = databaseName, tableSchema = "public")

    val excludedTables = Set.empty[String] // Set("pgbench_accounts")

    val excludedColumns: Map[String, Set[String]] = Map("pgbench_accounts" -> Set("bid"))

    val futures = extractor
      .columnMetadata                                                                                                  // form column metadata dataset
      .collect                                                                                                         // collect column metadata
      .filterNot(metadata => excludedTables.contains(metadata.table_name))                                             // filter excluded tables
      .filterNot(metadata => excludedColumns.getOrElse(metadata.table_name, Set.empty).contains(metadata.column_name)) // filter excluded columns
      .groupBy(_.table_name)                                                                                           // group by table name
      .map { mapping => processTable(spark, extractor, mapping._1, mapping._2) }                                       // retrieve the table

    Await.result(Future.sequence(futures), Duration(20, TimeUnit.MINUTES))
  }
}

/*
val mapping: Map[String, Map[String, DataType]] = columns.groupBy(x => x.table_name).flatMap { tuple =>
  tuple._2.map { column =>
    val colType: DataType = column.data_type match {
      case "integer" => IntegerType
      case "USER-DEFINED" => StringType
      case "timestamp without time zone" => TimestampType
      case "character" => StringType
      case "character varying" => StringType
      case _=> println("OOPS")
        StringType
    }
    //StructField(column.column_name, colType, nullable = true)
    tuple._1 -> column.column_name -> colType
  }

  /*
  val structType = StructType(cols)
  println(tuple._1)
  structType.printTreeString()
 */
  //((tuple._1), structType)
}
*/

/*
    tableMetadata.printSchema
    tableMetadata.show(false)
    tableMetadata.explain(extended = true)

    columnMetadata.printSchema
    columnMetadata.show(false)
    columnMetadata.explain(extended = true)
    */
