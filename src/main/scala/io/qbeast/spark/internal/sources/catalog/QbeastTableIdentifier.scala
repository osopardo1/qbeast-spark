package io.qbeast.spark.internal.sources.catalog

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession

/**
 * An identifier for a Delta table containing one of the path or the table identifier.
 */
case class QbeastTableIdentifier(
    path: Option[String] = None,
    table: Option[TableIdentifier] = None) {

  assert(
    path.isDefined ^ table.isDefined,
    "Please provide one of the path or the table identifier")

  val identifier: String = path.getOrElse(table.get.identifier)

  def database: Option[String] = table.flatMap(_.database)

  def getPath(spark: SparkSession): Path = {
    path.map(new Path(_)).getOrElse {
      val metadata = spark.sessionState.catalog.getTableMetadata(table.get)
      new Path(metadata.location)
    }
  }

  /**
   * Escapes back-ticks within the identifier name with double-back-ticks.
   */
  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  def quotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier)

    if (replacedDb.isDefined) s"`${replacedDb.get}`.`$replacedId`" else s"`$replacedId`"
  }

  def unquotedString: String = {
    if (database.isDefined) s"${database.get}.$identifier" else identifier
  }

  override def toString: String = quotedString
}

/**
 * Utilities for DeltaTableIdentifier. TODO(burak): Get rid of these utilities. DeltaCatalog
 * should be the skinny-waist for figuring these things out.
 */
object QbeastTableIdentifier extends Logging {

  /**
   * Check the specified table identifier represents a Delta path.
   */
  def isQbeastPath(spark: SparkSession, identifier: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    def tableIsTemporaryTable = catalog.isTempView(identifier)
    def tableExists: Boolean = {
      try {
        catalog.databaseExists(identifier.database.get) && catalog.tableExists(identifier)
      } catch {
        case e: AnalysisException if gluePermissionError(e) =>
          logWarning(
            "Received an access denied error from Glue. Will check to see if this " +
              s"identifier ($identifier) is path based.",
            e)
          false
      }
    }

    spark.sessionState.conf.runSQLonFile &&
    new Path(identifier.table).isAbsolute &&
    QbeastCatalogUtils.isQbeastProvider(identifier.database) &&
    !tableIsTemporaryTable &&
    !tableExists
  }

  /**
   * Check whether the provided table name is a Delta table based on information from the Catalog.
   */
  def isQbeastTable(spark: SparkSession, tableName: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    val tableIsNotTemporaryTable = !catalog.isTempView(tableName)
    val tableExists = {
      (tableName.database.isEmpty || catalog.databaseExists(tableName.database.get)) &&
      catalog.tableExists(tableName)
    }
    tableIsNotTemporaryTable && tableExists && QbeastCatalogUtils.isQbeastProvider(
      catalog.getTableMetadata(tableName))
  }

  /**
   * Creates a [[QbeastTableIdentifier]] if the specified table identifier represents a Delta
   * table, otherwise returns [[None]].
   */
  def apply(spark: SparkSession, identifier: TableIdentifier): Option[QbeastTableIdentifier] = {
    if (isQbeastPath(spark, identifier)) {
      Some(QbeastTableIdentifier(path = Option(identifier.table)))
    } else if (isQbeastTable(spark, identifier)) {
      Some(QbeastTableIdentifier(table = Option(identifier)))
    } else {
      None
    }
  }

  /**
   * When users try to access Delta tables by path, e.g. delta.`/some/path`, we need to first
   * check if such a table exists in the MetaStore (due to Spark semantics :/). The Glue MetaStore
   * may return Access Denied errors during this check. This method matches on this failure mode.
   */
  def gluePermissionError(e: AnalysisException): Boolean = e.getCause match {
    case h: Exception
        if h.getClass.getName == "org.apache.hadoop.hive.ql.metadata.HiveException" =>
      Seq("AWSGlue", "AccessDeniedException").forall { kw =>
        h.getMessage.contains(kw)
      }
    case _ => false
  }

}
