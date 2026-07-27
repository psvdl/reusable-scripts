# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

%run /EnvSettings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Spark session configuration
# This cell sets Spark session settings to enable _Verti-Parquet_ and _Optimize on Write_.

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import *
import json
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import datetime

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")
spark.conf.set('spark.ms.autotune.queryTuning.enabled', 'true')

#Low shuffle for untouched rows during MERGE
spark.conf.set("spark.microsoft.delta.merge.lowShuffle.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # getAbfsPath()
# Gets the Azure Blob File System (ABFS) path of the OneLake medallion layer as URI abfss://workspaceId@onelake.dfs.fabric.microsoft.com/lakehouseID

# CELL ********************

def getAbfsPath(medallionLayer):
    # ##########################################################################################################################  
    # Function: getAbfsPath
    # Gets the Azure Blob File System (ABFS) path of the OneLake medallion layer
    # as URI abfss://workspaceId@onelake.dfs.fabric.microsoft.com/lakehouseID
    #
    # Parameters:
    #   medallionLayer = Medallion layer of data platform. Valid values are bronze, silver or gold.
    #
    # Returns:
    #   The ABFS URI as string
    # ##########################################################################################################################
    validMedallionLayer = ["bronze","silver","gold"]
    assert medallionLayer in validMedallionLayer, "Invalid medallion layer. Valid values are bronze, silver or gold"

    AbfsPath = None
    workspaceId = None
    lhName = None

    match medallionLayer:
        case "bronze":
            workspaceId = bronzeWorkspaceId
            lhName = bronzeLakehouseName
        case "silver":
            workspaceId = silverWorkspaceId
            lhName = silverLakehouseName
        case "gold":
            workspaceId = goldWorkspaceId
            lhName = goldLakehouseName

    lh= notebookutils.lakehouse.getWithProperties(lhName,workspaceId)
    abfsPath = lh.properties["abfsPath"]

    return abfsPath

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # readFile()
# Reads a data file from Lakehouse and returns as Spark dataframe

# CELL ********************

def readFile(medallionLayer,container, folder, file, colSeparator=None, headerFlag=None):
    # ##########################################################################################################################  
    # Function: readFile
    # Reads a data file from Lakehouse and returns as spark dataframe
    #
    # Parameters:
    #   medallionLayer = Medallion layer of data platform. Valid values are bronze, silver or gold.
    #   container = Container of Lakehouse. Default value 'Files'
    #   folder = Folder within container where data file resides. E.g 'raw-bronze/wwi/Sales/Orders/2013-01'
    #   file = File name of data file including and file extension. E.g 'Sales_Orders_2013-01-01_000000.parquet'
    #   colSeparator = Column separator for text files. Default value None
    #   headerFlag = boolean flag to indicate whether the text file has a header or not. Default value None
    #
    # Returns:
    #   A dataframe of the data file
    # ##########################################################################################################################
    validMedallionLayer = ["bronze","silver","gold"]
    assert medallionLayer in validMedallionLayer, "Invalid medallion layer. Valid values are bronze, silver or gold"
    assert container is not None, "container not provided"
    assert folder is not None, "folder not provided"
    assert file is not None, "file not provided"

    abfsPath = getAbfsPath(medallionLayer)
    relativePath = container + '/' + folder +'/' + file
    filePath = abfsPath + '/' + relativePath

    if ".csv" in file or ".txt" in file:
        df = spark.read.csv(path=filePath, sep=colSeparator, header=headerFlag, inferSchema="true")
    elif ".parquet" in file:
        df = spark.read.parquet(filePath)
    elif ".json" in file:
        df = spark.read.json(filePath, multiLine= True)
    elif ".orc" in file:
        df = spark.read.orc(filePath)
    else:
        df = spark.read.format("csv").load(filePath)

    df =df.dropDuplicates()
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # readMedallionLHTable()
# Retrieves a Lakehouse Table from the medallion layers, allowing for table filtering and specific column selection

# CELL ********************

def readMedallionLHTable(medallionLayer,tableRelativePath, filterCond=None, colList=None):
    # ##########################################################################################################################  
    # Function: readMedallionLHTable
    # Retrieves a Lakehouse Table from the medallion layers, allowing for table filtering and specific column selection
    #
    # Parameters:
    #   medallionLayer = Medallion layer of data platform. Valid values are bronze, silver or gold.
    #   tableRelativePath = Relative path of the LH table in format Tables/Schema/TableName
    #   filterCond = A valid filter condition for the table, passed as string. E.g "ColorName == 'Salmon'". Default value is None.
    #       If filterCond is None, the full table will be returned.
    #   colList = Columns to be selected, passed as list. E.g. ["ColorID","ColorName"]. Default value is None.
    #       If colList is None, all columns in the table will be returned.
    #
    # Returns:
    #   A dataframe containing the Lakehouse table.
    # ##########################################################################################################################
    validMedallionLayer = ["bronze","silver","gold"]
    assert medallionLayer in validMedallionLayer, "Invalid medallion layer. Valid values are bronze, silver or gold"
    abfsPath = getAbfsPath(medallionLayer)
    tablePath = abfsPath + '/' + tableRelativePath

    # check if table exists
    table = DeltaTable.forPath(spark,tablePath)
    assert table is not None, "Lakehouse table does not exist"

    df = spark.read.format("delta").load(tablePath)

    # Apply filter condition
    if filterCond is not None:
        df = df.filter(filterCond)

    # Select columns
    if colList is not None:
        df = df.select(colList)

    df =df.dropDuplicates()

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # readLHTable()
# Retrieves a any Lakehouse Table from OneLake, allowing for table filtering and specific column selection. Use this function to read from any Lakehouse in Onelake outside data platform for e.g. a mirrored database, Fabric SQL etc

# CELL ********************

def readLHTable(LakehouseName,tableRelativePath,WorkspaceID=None, filterCond=None, colList=None):
    # ##########################################################################################################################  
    # Function: readLHTable
    # Retrieves a any Lakehouse Table from OneLake, allowing for table filtering and specific column selection
    # Use this function to read from any Lakehouse in Onelake outside data platform for e.g a mirrored database, Fabric SQL etc
    #
    # Parameters:
    #   LakehouseName = Name of lakehouse where table is located
    #   tableRelativePath = Relative path of the table in format Tables/Schema/TableName
    #   WorkspaceID = ID of Fabric Workspace where lakehouse is located. Default is None
    #       If WorkspaceID is None, the default Lakhouse attached to the notebook will be used.
    #   filterCond = A valid filter condition for the table, passed as string. E.g "ColorName == 'Salmon'". Default value is None.
    #       If filterCond is None, the full table will be returned.
    #   colList = Columns to be selected, passed as list. E.g. ["ColorID","ColorName"]. Default value is None.
    #       If colList is None, all columns in the table will be returned.
    #
    # Returns:
    #   A dataframe containing the Lakehouse table.
    # ##########################################################################################################################
    lh = notebookutils.lakehouse.getWithProperties(LakehouseName,WorkspaceID)
    abfsPath = lh.properties["abfsPath"]
    tablePath = abfsPath + '/' + tableRelativePath

    # check if table exists
    table = DeltaTable.forPath(spark,tablePath)
    assert table is not None, "Lakehouse table does not exist"

    df = spark.read.format("delta").load(tablePath)

    # Apply filter condition
    if filterCond is not None:
        df = df.filter(filterCond)

    # Select columns
    if colList is not None:
        df = df.select(colList)

    df =df.dropDuplicates()

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # tableExists()
# Checks whether a table exists in the current lakehouse

# CELL ********************

def tableExists(tableName):
    # ##########################################################################################################################  
    # Function: tableExists
    # Checks whether a table exists in the current lakehouse
    #
    # Parameters:
    #   tableName = Table name in current lakehouse
    #
    # Returns:
    #   True if the table exists, otherwise False
    # ##########################################################################################################################
    tableName = tableName.replace(".","_")

    return spark.catalog.tableExists(tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # isDeltaTable()
# Checks whether a path in a medallion layer lakehouse contains a Delta Lake table

# CELL ********************

def isDeltaTable(medallionLayer, tableRelativePath):
    # ##########################################################################################################################  
    # Function: isDeltaTable
    # Checks whether a path in a medallion layer lakehouse contains a Delta Lake table
    #
    # Parameters:
    #   medallionLayer = Medallion layer of data platform. Valid values are bronze, silver or gold.
    #   tableRelativePath = Relative path of the LH table in format Tables/Schema/TableName
    #
    # Returns:
    #   True if the path contains a Delta Lake table, otherwise False
    # ##########################################################################################################################
    validMedallionLayer = ["bronze","silver","gold"]
    assert medallionLayer in validMedallionLayer, "Invalid medallion layer. Valid values are bronze, silver or gold"

    abfsPath = getAbfsPath(medallionLayer)
    tablePath = abfsPath + '/' + tableRelativePath

    return DeltaTable.isDeltaTable(spark, tablePath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # getRowCount()
# Gets the number of rows of a table in the current lakehouse, with optional filtering

# CELL ********************

def getRowCount(tableName, filterCond=None):
    # ##########################################################################################################################  
    # Function: getRowCount
    # Gets the number of rows of a table in the current lakehouse, with optional filtering
    #
    # Parameters:
    #   tableName = Table name in current lakehouse
    #   filterCond = A valid filter condition for the table, passed as string. E.g "ColorName == 'Salmon'". Default value is None.
    #
    # Returns:
    #   Row count as integer
    # ##########################################################################################################################
    tableName = tableName.replace(".","_")
    assert tableExists(tableName), "Table does not exist"

    df = spark.table(tableName)

    # Apply filter condition
    if filterCond is not None:
        df = df.filter(filterCond)

    rowCount = df.count()
    print("Row count of " + tableName + ": " + str(rowCount))

    return rowCount

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # getTableSchema()
# Gets the schema of a table in the current lakehouse and prints it in a tree format

# CELL ********************

def getTableSchema(tableName):
    # ##########################################################################################################################  
    # Function: getTableSchema
    # Gets the schema of a table in the current lakehouse and prints it in a tree format
    #
    # Parameters:
    #   tableName = Table name in current lakehouse
    #
    # Returns:
    #   The schema as StructType
    # ##########################################################################################################################
    tableName = tableName.replace(".","_")
    assert tableExists(tableName), "Table does not exist"

    schema = spark.table(tableName).schema
    spark.table(tableName).printSchema()

    return schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # getTableHistory()
# Gets the transaction history of a delta lake table - useful to find versions for time travel or restore, and to inspect operational metrics of past writes

# CELL ********************

def getTableHistory(tableName, lastN=None):
    # ##########################################################################################################################  
    # Function: getTableHistory
    # Gets the transaction history of a delta lake table. Use this to find a version number or
    # timestamp for readDeltaTimeTravel() or restoreDeltaTable(), and to inspect operational
    # metrics of past write operations.
    #
    # Parameters:
    #   tableName = Delta lake tableName in current lakehouse
    #   lastN = Number of most recent transactions to return. Default value is None, which returns the full history.
    #
    # Returns:
    #   A dataframe containing the table history
    # ##########################################################################################################################
    tableName = tableName.replace(".","_")

    deltaTable = DeltaTable.forName(spark,tableName)
    assert deltaTable is not None, "Delta lake table does not exist"

    if lastN is not None:
        history = deltaTable.history(lastN)
    else:
        history = deltaTable.history()

    return history

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # dropTable()
# Drops a table from the current lakehouse

# CELL ********************

def dropTable(tableName):
    # ##########################################################################################################################  
    # Function: dropTable
    # Drops a table from the current lakehouse
    #
    # Parameters:
    #   tableName = Table name in current lakehouse
    #
    # Returns:
    #   True if the table existed and was dropped, False if it did not exist
    # ##########################################################################################################################
    tableName = tableName.replace(".","_")
    existed = tableExists(tableName)

    spark.sql("DROP TABLE IF EXISTS " + tableName)

    return existed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # insertDelta()
# Inserts a dataframe to delta lake table. Creates a table with the schema of dataframe if the table doesn't already exist

# CELL ********************

def insertDelta (df, tableName, writeMode="append", mergeSchema=False):
    # ##########################################################################################################################  
    # Function: insertDelta
    # Inserts a dataframe to delta lake table. Creates a table with the schema of dataframe if the table doesn't already exist
    #
    # Parameters:
    #   df = Input dataframe
    #   tableName = Target tableName in current lakehouse
    #   writeMode = write mode. Valid values are "append", "overwrite". Default value "append"
    #   mergeSchema = boolean flag to evolve the target table schema with new columns found in the input
    #                 dataframe. Only applies to append mode. Default value False
    #
    # Returns:
    #   Json containing operational metrics.This is useful to get information like number of records inserted/updated.
    #   E.g payload
    #   {'numOutputRows': '4432', 'numOutputBytes': '87157', 'numFiles': '1'}
    # ##########################################################################################################################
    validMode =[ "append", "overwrite"]
    assert writeMode in validMode, "Invalid mode specified"

    # Creating a delta table with schema name not supported at the time of writing this code, so replacing schema name with "_". To be commented out when this
    #feature is available
    tableName = tableName.replace(".","_")

    #Get delta table reference
    DeltaTable.createIfNotExists(spark).tableName(tableName).addColumns(df.schema).execute()
    deltaTable = DeltaTable.forName(spark,tableName)
    assert deltaTable is not None, "Delta table does not exist"

    writer = df.write.format("delta").mode(writeMode)
    if mergeSchema and writeMode == "append":
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(tableName)

    stats = DeltaTable.forName(spark,tableName).history(1).select("OperationMetrics").first()[0]
    print(stats)

    return stats

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # computeRowHash()
# Computes a SHA-256 hash over the columns of a dataframe and adds it as a rowhash column. Used for change detection during upserts

# CELL ********************

def computeRowHash(df, rowHashColumn="RowHash", excludeColumns=None):
    # ##########################################################################################################################  
    # Function: computeRowHash
    # Computes a SHA-256 hash over the columns of a dataframe and adds it as a rowhash column.
    # A target row only needs to be updated when its rowhash differs from the source rowhash.
    #
    # Parameters:
    #   df = Input dataframe
    #   rowHashColumn = Name of the rowhash column to add. Default value "RowHash"
    #   excludeColumns = Columns to exclude from the hash, passed as list. Exclude key columns, watermark
    #                    columns and pure ingestion/audit columns that change on every load (e.g. IngestionTimestamp),
    #                    otherwise every row would be flagged as changed on every run. Default value None.
    #
    # Returns:
    #   The input dataframe with the rowhash column added
    # ##########################################################################################################################
    exclude = set(excludeColumns) if excludeColumns is not None else set()
    exclude.add(rowHashColumn)

    hashCols = sorted([c for c in df.columns if c not in exclude])
    assert len(hashCols) > 0, "No columns available to compute rowhash"

    hashExpr = sha2(
        concat_ws("||", *[coalesce(col("`" + c + "`").cast("string"), lit("<NULL>")) for c in hashCols]),
        256
    )

    return df.withColumn(rowHashColumn, hashExpr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # dedupeDataframe()
# Removes duplicate business keys from a dataframe so a MERGE never matches the same target row more than once

# CELL ********************

def dedupeDataframe(df, keyColumns, orderByColumn=None):
    # ##########################################################################################################################  
    # Function: dedupeDataframe
    # Removes duplicate business keys from a dataframe. A MERGE fails when multiple source rows match
    # the same target row, so the source must contain only one row per business key.
    #
    # Parameters:
    #   df = Input dataframe
    #   keyColumns = Business key column(s), pipe-delimited string or list. E.g. "OrderID|LineID"
    #   orderByColumn = Column used to decide which duplicate wins - the row with the highest value is kept
    #                   (e.g. a watermark or last-modified column). Default value None, in which case an
    #                   arbitrary duplicate is kept.
    #
    # Returns:
    #   A dataframe with one row per business key
    # ##########################################################################################################################
    keyColumnsList = keyColumns.split("|") if isinstance(keyColumns, str) else list(keyColumns)
    assert len(keyColumnsList) > 0, "keyColumns not provided"

    if orderByColumn is not None:
        w = Window.partitionBy(*keyColumnsList).orderBy(col(orderByColumn).desc())
        df = (df.withColumn("__dedupe_rank", row_number().over(w))
                .filter(col("__dedupe_rank") == 1)
                .drop("__dedupe_rank"))
    else:
        df = df.dropDuplicates(keyColumnsList)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # upsertDelta()
# Upserts a dataframe to delta lake table. Inserts records that don't exist and updates existing records ONLY when their rowhash has changed

# CELL ********************

def upsertDelta(df, tableName, keyColumns, watermarkColumn=None, rowHashColumn="RowHash", deleteMissing=False, dedupeSource=True):
    # ##########################################################################################################################################
    # Function: upsertDelta
    # Upserts a dataframe to delta lake table. Inserts records that don't exist and updates existing
    # records ONLY when their rowhash has changed, i.e. when something in the row actually changed.
    #
    # Parameters:
    #   df = Input dataframe
    #   tableName = Target tableName in current lakehouse
    #   keyColumns = Business key column(s) used to match source and target rows, pipe-delimited string
    #                or list. E.g. "OrderID|LineID"
    #   watermarkColumn = Column used to protect against late-arriving older data. When provided, a changed
    #                     row is only updated if the source watermark is the same or newer than the target.
    #                     Default value None
    #   rowHashColumn = Name of the column used for change detection. If the source dataframe already carries
    #                   this column it is used as-is, otherwise it is computed as a SHA-256 hash over all
    #                   non-key columns (the watermark column is excluded from the hash). Default value "RowHash"
    #   deleteMissing = boolean flag to HARD DELETE target rows that are not present in the source.
    #                   Only use this for full loads, never for incremental loads. Default value False
    #   dedupeSource = boolean flag to remove duplicate business keys from the source before merging. When
    #                  watermarkColumn is provided the row with the highest watermark is kept. Default value True
    #
    # Returns:
    #   Json containing operational metrics.This is useful to get information like number of records inserted/updated.
    #   E.g payload
    #   {'numOutputRows': '4432', 'numTargetRowsInserted': '0', 'numTargetFilesAdded': '1',
    #    'numTargetFilesRemoved': '1', 'executionTimeMs': '2898', 'unmodifiedRewriteTimeMs': '606',
    #    'numTargetRowsCopied': '0', 'rewriteTimeMs': '921', 'numTargetRowsUpdated': '4432', 'numTargetRowsDeleted': '0',
    #    'scanTimeMs': '1615', 'numSourceRows': '4432', 'numTargetChangeFilesAdded': '0'}
    # ##########################################################################################################################################

    # Creating a delta table with schema name not supported at the time of writing this code, so replacing schema name with "_". To be commented out when this
    #feature is available
    tableName = tableName.replace(".","_")

    keyColumnsList = keyColumns.split("|") if isinstance(keyColumns, str) else list(keyColumns)
    assert len(keyColumnsList) > 0, "keyColumns not provided"

    # 1. Remove duplicate business keys from source so MERGE cannot match the same target row twice
    if dedupeSource:
        df = dedupeDataframe(df, keyColumnsList, watermarkColumn)

    # 2. Rowhash for change detection - use the existing column or compute it
    if rowHashColumn not in df.columns:
        excludeCols = keyColumnsList + ([watermarkColumn] if watermarkColumn is not None else [])
        df = computeRowHash(df, rowHashColumn, excludeCols)
        print("Rowhash column '" + rowHashColumn + "' not found in source - computed as SHA-256 over non-key columns")

    # 3. Get target table reference (schema includes the rowhash column for new tables)
    DeltaTable.createIfNotExists(spark).tableName(tableName).addColumns(df.schema).execute()
    target = DeltaTable.forName(spark,tableName)
    assert target is not None, "Target delta lake table does not exist"

    # 4. Add the rowhash column to a pre-existing target table if missing.
    #    Existing rows get NULL and are updated once on this merge to backfill the rowhash.
    if rowHashColumn not in target.toDF().columns:
        spark.sql("ALTER TABLE " + tableName + " ADD COLUMNS (`" + rowHashColumn + "` STRING)")
        print("Added column '" + rowHashColumn + "' to existing table '" + tableName + "' - existing rows will be updated once to backfill the rowhash")
        target = DeltaTable.forName(spark,tableName)

    # 5. Merge condition on business keys only.
    #    The watermark belongs to the UPDATE condition below - putting it in the merge condition would
    #    treat older source rows as unmatched and attempt a duplicate-key INSERT.
    joinCond = " AND ".join(["target.`" + keyCol + "` = source.`" + keyCol + "`" for keyCol in keyColumnsList])

    # 6. Update only when the rowhash changed (null-safe compare), optionally guarded by the watermark
    updateCond = "NOT (source.`" + rowHashColumn + "` <=> target.`" + rowHashColumn + "`)"
    if watermarkColumn is not None:
        updateCond = updateCond + " AND target.`" + watermarkColumn + "` <= source.`" + watermarkColumn + "`"

    # Column mappings for insert and update
    updateStatement = {c: "source.`" + c + "`" for c in df.columns}
    insertStatement = {c: "source.`" + c + "`" for c in df.columns}

    merger = (target.alias("target")
        .merge(df.alias('source'), joinCond)
        .whenMatchedUpdate(condition = updateCond, set = updateStatement)
        .whenNotMatchedInsert(values = insertStatement)
    )

    if deleteMissing:
        merger = merger.whenNotMatchedBySourceDelete()

    merger.execute()

    stats = target.history(1).select("OperationMetrics").first()[0]
    print(stats)

    return stats

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # getHighWaterMark()
# Retrieves High Watermark from a Lakehouse table for a given date range

# CELL ********************

def getHighWaterMark(lakehouseName,tableRelativePath,watermarkColName, fromRange, toRange, workspaceID=None):
    # ##########################################################################################################################  
    # Function: getHighWaterMark
    # Retrieves High Watermark from a Lakehouse table for a given date range
    #
    # Parameters:
    #   lakehouseName = Name of lakehouse where table is located
    #   tableRelativePath = Relative path of the table in format Tables/Schema/TableName
    #   watermarkColName - Name of column used for watermark
    #   fromRange - datetime of lower range of data in watermark column
    #   toRange - datetime of upper range of data in watermark column
    #   WorkspaceID = ID of Fabric Workspace where lakehouse is located. Default is None
    #       If WorkspaceID is None, the default Lakhouse attached to the notebook will be used.
    #
    # Returns:
    #   A max value of the high watermark column for the datetime range
    # ##########################################################################################################################
    assert watermarkColName is not None,"Watermark column name not provided"
    assert fromRange is not None,"fromRange datetime not provided"
    assert toRange is not None,"toRange datetime not provided"

    filterCond = watermarkColName +">" + "'" + fromRange +"'" + " and " + watermarkColName + "<=" + "'" + toRange + "'"
    df = readLHTable(lakehouseName,tableRelativePath,workspaceID,filterCond,watermarkColName)
    hwm = df.agg({watermarkColName: "max"}).collect()[0][0]

    return hwm

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # optimizeDelta()
# Compacts small files, optionally Z-ORDERs data and removes unused files beyond a configurable retention period for a Delta Table

# CELL ********************

def optimizeDelta(tableName, zOrderBy=None, retentionHours=None):
    # ##########################################################################################################################################
    # Function: optimizeDelta
    # Function that implements the compaction of small files for Delta Tables https://docs.delta.io/latest/optimizations-oss.html#language-python
    # Optionally co-locates related data with Z-ORDER BY, removes unused files beyond the retention period
    # and runs REORG TABLE to further optimize query performance.
    #
    # Parameters:
    #   tableName = Delta lake tableName in current lakehouse
    #   zOrderBy = Columns to Z-ORDER BY, pipe-delimited string or list. E.g. "OrderDate|CustomerID".
    #              Default value None, in which case only compaction is performed.
    #   retentionHours = Retention period in hours for VACUUM. Default value None, which uses the delta table
    #                    default (168 hours / 7 days). Values below 168 hours are permitted, but weaken the
    #                    safety guarantee for time travel and concurrent readers.
    #
    # Returns:
    #   None
    # ##########################################################################################################################################
    # Creating a delta table with schema name not supported at the time of writing this code, so replacing schema name with "_". To be commented out when this
    #feature is available
    tableName = tableName.replace(".","_")

    deltaTable = DeltaTable.forName(spark,tableName)
    assert deltaTable is not None, "Delta lake table does not exist"

    # Compact small files, optionally with Z-ORDER BY
    if zOrderBy is not None:
        zOrderCols = zOrderBy.split("|") if isinstance(zOrderBy, str) else list(zOrderBy)
        deltaTable.optimize().executeZOrderBy(zOrderCols)
    else:
        deltaTable.optimize().executeCompaction()

    # Remove unused files beyond retention period
    if retentionHours is not None:
        if retentionHours < 168:
            spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            print("WARNING: VACUUM retention below 168 hours disables the default safety check - time travel beyond the retention period will no longer be possible")
        deltaTable.vacuum(retentionHours)
    else:
        deltaTable.vacuum()

    # Run REORG TABLE for additional query performance optimization (Lakehouse SQL optimization)
    spark.sql(f"REORG TABLE {tableName} APPLY (PURGE)")

    return

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # readDeltaTimeTravel()
# Reads a delta lake table as it was at a given version or point in time (time travel)

# CELL ********************

def readDeltaTimeTravel(tableName, version=None, timestamp=None):
    # ##########################################################################################################################  
    # Function: readDeltaTimeTravel
    # Reads a delta lake table as it was at a given version or point in time (time travel).
    # Use getTableHistory() to find a valid version number or timestamp.
    #
    # Parameters:
    #   tableName = Delta lake tableName in current lakehouse
    #   version = Version number to read. E.g. 5. Provide either version or timestamp, not both.
    #   timestamp = Timestamp to read, as ISO-8601 string. E.g. "2024-06-30T23:59:59".
    #               Provide either version or timestamp, not both.
    #
    # Returns:
    #   A dataframe of the table at the given version or timestamp
    # ##########################################################################################################################
    assert (version is None) != (timestamp is None), "Provide exactly one of version or timestamp"

    tableName = tableName.replace(".","_")
    assert tableExists(tableName), "Delta lake table does not exist"

    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    else:
        reader = reader.option("timestampAsOf", timestamp)

    return reader.table(tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # restoreDeltaTable()
# Restores a delta lake table to an earlier version or point in time

# CELL ********************

def restoreDeltaTable(tableName, version=None, timestamp=None):
    # ##########################################################################################################################  
    # Function: restoreDeltaTable
    # Restores a delta lake table to an earlier version or point in time, e.g. to recover from a
    # bad load. Use getTableHistory() to find the version number or timestamp to restore to.
    #
    # Parameters:
    #   tableName = Delta lake tableName in current lakehouse
    #   version = Version number to restore to. E.g. 5. Provide either version or timestamp, not both.
    #   timestamp = Timestamp to restore to, as ISO-8601 string. E.g. "2024-06-30T23:59:59".
    #               Provide either version or timestamp, not both.
    #
    # Returns:
    #   Json containing operational metrics of the RESTORE operation
    # ##########################################################################################################################
    assert (version is None) != (timestamp is None), "Provide exactly one of version or timestamp"

    tableName = tableName.replace(".","_")

    deltaTable = DeltaTable.forName(spark,tableName)
    assert deltaTable is not None, "Delta lake table does not exist"

    if version is not None:
        deltaTable.restoreToVersion(version)
    else:
        deltaTable.restoreToTimestamp(timestamp)

    stats = DeltaTable.forName(spark,tableName).history(1).select("OperationMetrics").first()[0]
    print(stats)

    return stats

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # readMirrorDBTable()
# Read from a mirrored DB table using a 4-part name: **_workspaceName.mirrorDBName.schemaName.tableName_**

# CELL ********************

def readMirrorDBTable(
    workspaceName: str,
    mirrorDBName: str,
    schemaName: str,
    tableName: str,
    watermarkColumnName: str = None,
    fromTimeStamp: str = None,
    toTimeStamp: str = None,
) -> DataFrame:

    """
    Read from a mirrored DB table using a 4-part name:
    workspaceName.mirrorDBName.schemaName.tableName

    Required:
        workspaceName, mirrorDBName, schemaName, tableName

    Optional:
        watermarkColumnName, fromTimeStamp, toTimeStamp
        If fromTimeStamp/toTimeStamp are provided, a WHERE clause on watermarkColumnName
        is added with the corresponding range.
    """

    # ---- Validation of required parameters ----
    for param_name, param_value in [
        ("workspaceName", workspaceName),
        ("mirrorDBName", mirrorDBName),
        ("schemaName", schemaName),
        ("tableName", tableName),
    ]:
        if not isinstance(param_value, str) or not param_value.strip():
            raise ValueError(f"Parameter '{param_name}' is required and must be a non-empty string.")

    # # ---- Validation for watermark usage ----
    # if (fromTimeStamp or toTimeStamp) and not watermarkColumnName:
    #     raise ValueError(
    #         "Parameter 'watermarkColumnName' is required when "
    #         "'fromTimeStamp' or 'toTimeStamp' is provided."
    #     )

    # ---- Validation: fromTimeStamp must be earlier than toTimeStamp ----
    if fromTimeStamp and toTimeStamp:
        try:
            from_dt = datetime.datetime.fromisoformat(fromTimeStamp)
            to_dt = datetime.datetime.fromisoformat(toTimeStamp)
        except ValueError:
            raise ValueError(
                "fromTimeStamp and toTimeStamp must be ISO-8601 datetime strings, "
                "e.g. '2013-01-01T00:00:00'."
            )
        if from_dt > to_dt:
            raise ValueError("fromTimeStamp must be earlier than toTimeStamp.")

    # Helper to safely quote identifiers (handles dashes etc.)
    def quote_identifier(identifier: str) -> str:
        # Escape any backticks inside the identifier
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    full_table_name = ".".join([
        quote_identifier(workspaceName),
        quote_identifier(mirrorDBName),
        quote_identifier(schemaName),
        quote_identifier(tableName),
    ])

    where_clauses = []

    if watermarkColumnName:
        col_expr = quote_identifier(watermarkColumnName)

        # Build range conditions if timestamps are provided
        if fromTimeStamp:
            # assuming timestamp literals are passed as strings compatible with Spark (e.g. '2024-01-01T00:00:00')
            where_clauses.append(f"{col_expr} >= TIMESTAMP '{fromTimeStamp}'")
        if toTimeStamp:
            where_clauses.append(f"{col_expr} < TIMESTAMP '{toTimeStamp}'")

    # Base query
    query = f"SELECT * FROM {full_table_name}"

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    # Execute via Spark SQL and return DataFrame
    df = spark.sql(query)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
