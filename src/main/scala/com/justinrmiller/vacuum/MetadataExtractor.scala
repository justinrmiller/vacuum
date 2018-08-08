package com.justinrmiller.vacuum

import org.apache.spark.sql.Dataset

case class TableMetadata(table_catalog: String, table_schema: String, table_name: String)

case class ColumnMetadata(table_catalog: String, table_schema: String, table_name: String, column_name: String, data_type: String)

abstract class MetadataExtractor extends Serializable {
  def url: String

  def options: Map[String, String]

  def tableMetadata: Dataset[TableMetadata]

  def columnMetadata: Dataset[ColumnMetadata]
}
