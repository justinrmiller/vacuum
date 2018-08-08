package com.justinrmiller.vacuum

import org.apache.spark.sql.{Dataset, SparkSession}


class PostgresMetadataExtractor(
                                 session: SparkSession,
                                 databaseName: String,
                                 tableSchema: String
                               ) extends MetadataExtractor {

  import session.implicits._

  private val databaseType: String = "postgresql"
  private val tableMetadataTable: String = "information_schema.tables"
  private val columnMetadataTable: String = "information_schema.columns"

  private val tableMetadataColumns: Seq[String] = Seq("table_catalog", "table_schema", "table_name")

  private val columnMetadataColumns: Seq[String] = Seq("table_catalog", "table_schema", "table_name", "column_name", "data_type")

  override def url: String = s"jdbc:$databaseType:$databaseName"

  override def options: Map[String, String] = {
    Map(
      "url" -> s"jdbc:$databaseType:$databaseName",
      "dbtable" -> tableMetadataTable
    )
  }

  override def tableMetadata: Dataset[TableMetadata] = session
    .read
    .format("jdbc")
    .options(options + ("dbtable" -> tableMetadataTable))
    .load
    .select(tableMetadataColumns.map(col => $"$col"): _*)
    .filter($"table_schema" === tableSchema).as[TableMetadata]

  override def columnMetadata: Dataset[ColumnMetadata] = session
    .read
    .format("jdbc")
    .options(options + ("dbtable" -> columnMetadataTable))
    .load
    .select(columnMetadataColumns.map(col => $"$col"): _*)
    .filter($"table_schema" === tableSchema).as[ColumnMetadata]
}