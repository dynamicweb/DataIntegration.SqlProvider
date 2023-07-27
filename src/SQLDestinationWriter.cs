using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{
    /// <summary>
    /// Sql destination writer
    /// </summary>
    public class SqlDestinationWriter : IDestinationWriter, IDisposable
    {
        public SqlCommand SqlCommand;

        private Mapping mapping;
        public Mapping Mapping
        {
            get { return mapping; }
        }

        /// <summary>
        /// Return rows to write count
        /// </summary>
        public int RowsToWriteCount
        {
            get
            {
                return rowsToWriteCount;
            }
        }

        protected SqlBulkCopy SqlBulkCopier;
        protected DataSet DataToWrite = new DataSet();
        protected DataTable TableToWrite;
        protected readonly ILogger logger;
        protected int rowsToWriteCount;
        protected int lastLogRowsCount;
        protected int SkippedFailedRowsCount;
        protected readonly bool removeMissingAfterImport;
        protected readonly string tempTablePrefix = "TempTableForSqlProviderImport";
        protected readonly bool discardDuplicates;
        protected DuplicateRowsHandler duplicateRowsHandler;
        protected readonly bool removeMissingAfterImportDestinationTablesOnly;
        protected readonly bool SkipFailingRows;

        /// <summary>
        /// Initializes a new instance of the SqlDestinationWriter class.
        /// </summary>        
        /// <param name="mapping">Mapping</param>
        /// <param name="connection">Connection</param>
        /// <param name="removeMissingAfterImport">Delete rows not present in the import</param>
        /// <param name="logger">Logger instance</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="discardDuplicates">Discard duplicates</param>
        /// <param name="removeMissingAfterImportDestinationTablesOnly">Remove missing rows after import in the destination tables only</param>
        public SqlDestinationWriter(Mapping mapping, SqlConnection connection, bool removeMissingAfterImport, ILogger logger, string tempTablePrefix, bool discardDuplicates, bool removeMissingAfterImportDestinationTablesOnly)
            : this(mapping, connection, removeMissingAfterImport, logger, tempTablePrefix, discardDuplicates)
        {
            this.removeMissingAfterImportDestinationTablesOnly = removeMissingAfterImportDestinationTablesOnly;
        }

        /// <summary>
        /// Initializes a new instance of the SqlDestinationWriter class.
        /// </summary>        
        /// <param name="mapping">Mapping</param>
        /// <param name="connection">Connection</param>
        /// <param name="removeMissingAfterImport">Delete rows not present in the import</param>
        /// <param name="logger">Logger instance</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="discardDuplicates">Discard duplicates</param>
        /// <param name="removeMissingAfterImportDestinationTablesOnly">Remove missing rows after import in the destination tables only</param>
        /// <param name="skipFailingRows">Skip failing rows</param>
        public SqlDestinationWriter(Mapping mapping, SqlConnection connection, bool removeMissingAfterImport, ILogger logger, string tempTablePrefix, bool discardDuplicates, bool removeMissingAfterImportDestinationTablesOnly, bool skipFailingRows)
            : this(mapping, connection, removeMissingAfterImport, logger, tempTablePrefix, discardDuplicates)
        {
            SkipFailingRows = skipFailingRows;
            this.removeMissingAfterImportDestinationTablesOnly = removeMissingAfterImportDestinationTablesOnly;
        }

        /// <summary>
        /// Initializes a new instance of the SqlDestinationWriter class.
        /// </summary>        
        /// <param name="mapping">Mapping</param>
        /// <param name="connection">Connection</param>
        /// <param name="removeMissingAfterImport">Delete rows not present in the import</param>
        /// <param name="logger">Logger instance</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="discardDuplicates">Discard duplicates</param>
        public SqlDestinationWriter(Mapping mapping, SqlConnection connection, bool removeMissingAfterImport, ILogger logger, string tempTablePrefix, bool discardDuplicates)
        {
            this.mapping = mapping;
            SqlCommand = connection.CreateCommand();
            SqlCommand.CommandTimeout = 1200;
            this.removeMissingAfterImport = removeMissingAfterImport;
            this.logger = logger;
            this.tempTablePrefix = tempTablePrefix;
            this.discardDuplicates = discardDuplicates;
            SqlBulkCopier = new SqlBulkCopy(connection);
            SqlBulkCopier.DestinationTableName = mapping.DestinationTable.Name + this.tempTablePrefix;
            SqlBulkCopier.BulkCopyTimeout = 0;
            Initialize();
            //this must be after Initialize() as the connection may be closed in DuplicateRowsHandler->GetOriginalSourceSchema
            if (connection.State != ConnectionState.Open)
                connection.Open();
        }

        /// <summary>
        /// Initializes a new instance of the SqlDestinationWriter class.
        /// </summary>
        /// <param name="mapping">Mapping</param>
        /// <param name="connection">Connection</param>
        /// <param name="removeMissingAfterImport">Delete rows not present in the import</param>
        /// <param name="logger">Logger instance</param>
        /// <param name="discardDuplicates">Discard duplicates</param>
        public SqlDestinationWriter(Mapping mapping, SqlConnection connection, bool removeMissingAfterImport, ILogger logger, bool discardDuplicates)
        {
            this.mapping = mapping;
            SqlCommand = connection.CreateCommand();
            SqlCommand.CommandTimeout = 1200;
            this.removeMissingAfterImport = removeMissingAfterImport;
            this.logger = logger;
            this.discardDuplicates = discardDuplicates;
            SqlBulkCopier = new SqlBulkCopy(connection);
            SqlBulkCopier.DestinationTableName = mapping.DestinationTable.Name + tempTablePrefix;
            SqlBulkCopier.BulkCopyTimeout = 0;
            Initialize();
            //this must be after Initialize() as the connection may be closed in DuplicateRowsHandler->GetOriginalSourceSchema
            if (connection.State != ConnectionState.Open)
                connection.Open();
        }

        /// <summary>
        /// Initializes a new instance of the SqlDestinationWriter class.
        /// </summary>
        /// <param name="mapping">Mapping</param>
        /// <param name="mockSqlCommand">Mock SqlCommand</param>
        /// <param name="removeMissingAfterImport">Delete rows not present in the import</param>
        /// <param name="logger">Logger instance</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="discardDuplicates">Discard duplicates</param>
        public SqlDestinationWriter(Mapping mapping, SqlCommand mockSqlCommand, bool removeMissingAfterImport, ILogger logger, string tempTablePrefix, bool discardDuplicates)
        {
            this.mapping = mapping;
            SqlCommand = mockSqlCommand;
            this.removeMissingAfterImport = removeMissingAfterImport;
            this.logger = logger;
            this.tempTablePrefix = tempTablePrefix;
            this.discardDuplicates = discardDuplicates;
            Initialize();
        }

        protected virtual void Initialize()
        {
            List<SqlColumn> destColumns = new List<SqlColumn>();
            var columnMappings = Mapping.GetColumnMappings();
            foreach (ColumnMapping columnMapping in columnMappings.DistinctBy(obj => obj.DestinationColumn.Name))
            {
                destColumns.Add((SqlColumn)columnMapping.DestinationColumn);
            }
            if (Mapping.DestinationTable != null && Mapping.DestinationTable.Name == "EcomAssortmentPermissions")
            {
                if (columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAccessUserID", true) == 0) == null)
                    destColumns.Add(new SqlColumn("AssortmentPermissionAccessUserID", typeof(string), SqlDbType.Int, null, -1, false, true, false));
            }
            SQLTable.CreateTempTable(SqlCommand, Mapping.DestinationTable.SqlSchema, Mapping.DestinationTable.Name, tempTablePrefix, destColumns, logger);

            TableToWrite = DataToWrite.Tables.Add(Mapping.DestinationTable.Name + tempTablePrefix);
            foreach (SqlColumn column in destColumns)
            {
                TableToWrite.Columns.Add(column.Name, column.Type);
            }
            if (discardDuplicates)
            {
                duplicateRowsHandler = new DuplicateRowsHandler(logger, Mapping);
            }
        }

        /// <summary>
        /// Writes the specified row.
        /// </summary>
        /// <param name="Row">The row to be written.</param>
        public virtual void Write(Dictionary<string, object> row)
        {
            if (!mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }

            DataRow dataRow = TableToWrite.NewRow();

            var columnMappings = mapping.GetColumnMappings().Where(cm => cm.Active);
            foreach (ColumnMapping columnMapping in columnMappings)
            {
                object rowValue = null;
                if (columnMapping.HasScriptWithValue || row.TryGetValue(columnMapping.SourceColumn?.Name, out rowValue))
                {
                    object dataToRow = columnMapping.ConvertInputValueToOutputValue(rowValue);

                    if (columnMappings.Any(obj => obj.DestinationColumn.Name == columnMapping.DestinationColumn.Name && obj.GetId() != columnMapping.GetId()))
                    {
                        dataRow[columnMapping.DestinationColumn.Name] += dataToRow.ToString();
                    }
                    else
                    {
                        dataRow[columnMapping.DestinationColumn.Name] = dataToRow;
                    }
                }
                else
                {
                    throw new Exception(BaseDestinationWriter.GetRowValueNotFoundMessage(row, columnMapping.SourceColumn.Table.Name, columnMapping.SourceColumn.Name));
                }
            }

            if (!discardDuplicates || !duplicateRowsHandler.IsRowDuplicate(columnMappings, mapping, dataRow, row))
            {
                TableToWrite.Rows.Add(dataRow);

                if (TableToWrite.Rows.Count >= 1000)
                {
                    rowsToWriteCount = rowsToWriteCount + TableToWrite.Rows.Count;
                    SqlBulkCopierWriteToServer();
                    rowsToWriteCount = rowsToWriteCount - SkippedFailedRowsCount;
                    TableToWrite.Clear();
                    if (rowsToWriteCount >= lastLogRowsCount + 10000)
                    {
                        lastLogRowsCount = rowsToWriteCount;
                        logger.Log("Added " + rowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
                    }
                }
            }
        }

        /// <summary>
        /// Deletes rows not present in the import source
        /// </summary>
        /// <param name="extraConditions">Where condition to filter data for deletion</param>
        public virtual void DeleteExcessFromMainTable(string extraConditions)
        {
            if (removeMissingAfterImport || removeMissingAfterImportDestinationTablesOnly)
            {
                DeleteExcessFromMainTable(Mapping, extraConditions, SqlCommand, tempTablePrefix, removeMissingAfterImportDestinationTablesOnly);
            }
        }

        /// <summary>
        /// Deletes rows present/not present in the import source
        /// </summary>
        /// <param name="deleteExistingRows">If true deletes existing rows present in the import source. If false deletes excess rows not present in the import source.</param>
        /// <param name="mapping">Mapping</param>
        /// <param name="extraConditions">Where condition to filter data for deletion</param>
        /// <param name="sqlCommand">Command instance to execute the sql delete statement</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="languageID">the language to layer to delete from</param>
        private static void DeleteRowsFromMainTable(bool deleteExistingRows, Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            try
            {

                StringBuilder sqlClean = new StringBuilder("DECLARE @r INT; SET @r = 1; WHILE @r > 0 BEGIN ");
                sqlClean.Append("Delete top(100000) from [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + "] where ");

                if (!deleteExistingRows)
                {
                    sqlClean.Append("not ");
                }
                sqlClean.Append("exists  (select * from [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + tempTablePrefix + "] where ");

                sqlClean.Append(GetWhereConditionForDelete(mapping));
                sqlClean.Append(")");
                if (extraConditions.Length > 0)
                    sqlClean.Append(extraConditions);

                List<SqlParameter> parameters = new List<SqlParameter>();

                if (mapping.Conditionals.Count > 0)
                {
                    string mappingConditions = MappingExtensions.GetConditionalsSql(out parameters, mapping.Conditionals, true, true);
                    if (!string.IsNullOrEmpty(mappingConditions))
                    {
                        mappingConditions = mappingConditions.Substring(0, mappingConditions.Length - 4);
                        sqlClean.AppendFormat(" and ( {0} ) ", mappingConditions);
                        sqlCommand.Parameters.Clear();
                        foreach (SqlParameter p in parameters)
                            sqlCommand.Parameters.Add(p);
                    }
                }
                sqlClean.Append(" SET @r = @@ROWCOUNT; END");

                sqlCommand.CommandText = sqlClean.ToString();
                sqlCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string msg = "Failed to remove rows from Table [" + mapping.DestinationTable.SqlSchema + "." + mapping.DestinationTable.Name + "] that where ";
                if (!deleteExistingRows)
                {
                    msg += "not ";
                }
                msg += "present in source. Exception message: " + ex.Message + " Sql query: " + sqlCommand.CommandText;
                throw new Exception(msg, ex);
            }
        }

        /// <summary>
        /// Deletes rows not present in the import source
        /// </summary>
        /// <param name="mapping">Mapping</param>
        /// <param name="extraConditions">Where condition to filter data for deletion</param>
        /// <param name="sqlCommand">Command instance to execute the sql delete statement</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="languageID">the language to layer to delete from</param>        
        public static void DeleteExcessFromMainTable(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            DeleteExcessFromMainTable(mapping, extraConditions, sqlCommand, tempTablePrefix, false);
        }

        /// <summary>
        /// Deletes rows not present in the import source
        /// </summary>
        /// <param name="mapping">Mapping</param>
        /// <param name="extraConditions">Where condition to filter data for deletion</param>
        /// <param name="sqlCommand">Command instance to execute the sql delete statement</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="languageID">the language to layer to delete from</param>
        /// <param name="removeMissingAfterImportDestinationTablesOnly">Remove missing rows after import in the destination tables only</param>
        public static void DeleteExcessFromMainTable(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix, bool removeMissingAfterImportDestinationTablesOnly)
        {
            bool? removeMissing = mapping.GetOptionValue("RemoveMissingAfterImport");
            if (!removeMissing.HasValue || removeMissing.Value || removeMissingAfterImportDestinationTablesOnly)
            {
                if (!removeMissingAfterImportDestinationTablesOnly)
                {
                    DeleteExcessFromRelationTables(mapping, extraConditions, sqlCommand, tempTablePrefix);
                }
                DeleteRowsFromMainTable(false, mapping, extraConditions, sqlCommand, tempTablePrefix);
            }
        }

        /// <summary>
        /// Deletes existing rows present in the import source
        /// </summary>
        /// <param name="mapping">Mapping</param>
        /// <param name="extraConditions">Where condition to filter data for deletion</param>
        /// <param name="sqlCommand">Command instance to execute the sql delete statement</param>
        /// <param name="tempTablePrefix">Temporary table prefix</param>
        /// <param name="languageID">the language to layer to delete from</param>
        public static void DeleteExistingFromMainTable(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            DeleteRowsFromMainTable(true, mapping, extraConditions, sqlCommand, tempTablePrefix);
        }

        /// <summary>
        /// Write data using SQL bulk copier
        /// </summary>
        public virtual void FinishWriting()
        {
            SqlBulkCopierWriteToServer();
            if (TableToWrite.Rows.Count != 0)
            {
                rowsToWriteCount = rowsToWriteCount + TableToWrite.Rows.Count - SkippedFailedRowsCount;
                logger.Log("Added " + rowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
            }
        }

        /// <summary>
        /// Close writer
        /// </summary>
        public virtual void Close()
        {
            string tableName = Mapping.DestinationTable.Name + tempTablePrefix;
            SqlCommand.CommandText = "if exists (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" + tableName + "') AND type in (N'U')) drop table " + tableName;
            SqlCommand.ExecuteNonQuery();
            ((IDisposable)SqlBulkCopier).Dispose();
            if (duplicateRowsHandler != null)
            {
                duplicateRowsHandler.Dispose();
            }
        }

        /// <summary>
        /// Move data to main table
        /// </summary>
        /// <param name="sqlTransaction">Transaction</param>
        public virtual void MoveDataToMainTable(SqlTransaction sqlTransaction)
        {
            MoveDataToMainTable(sqlTransaction, false);
        }

        /// <summary>
        /// Move data to main table
        /// </summary>
        /// <param name="sqlTransaction">Transaction</param>
        /// <param name="updateOnly">Update only</param>
        public virtual void MoveDataToMainTable(SqlTransaction sqlTransaction, bool updateOnly)
        {
            MoveDataToMainTable(sqlTransaction, updateOnly, false);
        }

        /// <summary>
        /// Move data to main table
        /// </summary>
        /// <param name="sqlTransaction">Transaction</param>
        /// <param name="updateOnly">Update only</param>
        /// <param name="insertOnly">Insert only</param>
        public virtual void MoveDataToMainTable(SqlTransaction sqlTransaction, bool updateOnly, bool insertOnly)
        {
            SqlCommand.Transaction = sqlTransaction;
            List<string> insertColumns = new List<string>();
            //Get columnList for current Table
            try
            {
                string sqlConditions = "";
                string firstKey = "";
                var columnMappings = Mapping.GetColumnMappings().Where(cm => cm.Active).DistinctBy(obj => obj.DestinationColumn.Name);
                bool isPrimaryKeyColumnExists = columnMappings.IsKeyColumnExists();

                foreach (ColumnMapping columnMapping in columnMappings)
                {
                    SqlColumn column = (SqlColumn)columnMapping.DestinationColumn;
                    if (column.IsKeyColumn(columnMappings) || (!isPrimaryKeyColumnExists && !columnMapping.ScriptValueForInsert))
                    {
                        sqlConditions = sqlConditions + "[" + Mapping.DestinationTable.SqlSchema + "].[" +
                                              Mapping.DestinationTable.Name + "].[" + columnMapping.DestinationColumn.Name + "]=[" +
                                              Mapping.DestinationTable.SqlSchema + "].[" +
                                              Mapping.DestinationTable.Name + tempTablePrefix + "].[" + columnMapping.DestinationColumn.Name + "] and ";
                        if (firstKey == "")
                            firstKey = columnMapping.DestinationColumn.Name;
                    }
                }
                sqlConditions = sqlConditions.Substring(0, sqlConditions.Length - 4);

                string selectColumns = "";
                string updateColumns = "";
                foreach (var columnMapping in columnMappings)
                {
                    insertColumns.Add("[" + columnMapping.DestinationColumn.Name + "]");
                    selectColumns = selectColumns + "[" + Mapping.DestinationTable.SqlSchema + "].[" + Mapping.DestinationTable.Name + tempTablePrefix + "].[" + columnMapping.DestinationColumn.Name + "], ";
                    if (!((SqlColumn)columnMapping.DestinationColumn).IsIdentity && !((SqlColumn)columnMapping.DestinationColumn).IsKeyColumn(columnMappings) && !columnMapping.ScriptValueForInsert)
                        updateColumns = updateColumns + "[" + columnMapping.DestinationColumn.Name + "]=[" + Mapping.DestinationTable.SqlSchema + "].[" + columnMapping.DestinationColumn.Table.Name + tempTablePrefix + "].[" + columnMapping.DestinationColumn.Name + "], ";
                }

                string sqlUpdateInsert = "";
                if (!string.IsNullOrEmpty(updateColumns) && !insertOnly)
                {
                    updateColumns = updateColumns.Substring(0, updateColumns.Length - 2);
                    sqlUpdateInsert = sqlUpdateInsert + "update [" + Mapping.DestinationTable.SqlSchema + "].[" + Mapping.DestinationTable.Name + "] set " + updateColumns + " from [" + Mapping.DestinationTable.SqlSchema + "].[" + Mapping.DestinationTable.Name + tempTablePrefix + "] where " + sqlConditions + ";";
                }
                if (!string.IsNullOrEmpty(selectColumns))
                {
                    selectColumns = selectColumns.Substring(0, selectColumns.Length - 2);
                    if (!updateOnly)
                    {
                        if (HasIdentity(Mapping))
                        {
                            sqlUpdateInsert = sqlUpdateInsert + "set identity_insert [" + Mapping.DestinationTable.SqlSchema + "].[" +
                                                 Mapping.DestinationTable.Name + "] ON;";
                        }
                        sqlUpdateInsert = sqlUpdateInsert + " insert into [" + Mapping.DestinationTable.SqlSchema + "].[" + Mapping.DestinationTable.Name + "] (" + string.Join(",", insertColumns) + ") (" +
                            "select " + selectColumns + " from [" + Mapping.DestinationTable.SqlSchema + "].[" + Mapping.DestinationTable.Name + tempTablePrefix + "] left outer join [" + Mapping.DestinationTable.SqlSchema + "].[" + Mapping.DestinationTable.Name + "] on " + sqlConditions + " where [" + Mapping.DestinationTable.SqlSchema + "].[" + Mapping.DestinationTable.Name + "].[" + firstKey + "] is null);";
                        if (HasIdentity(Mapping))
                        {
                            sqlUpdateInsert = sqlUpdateInsert + "set identity_insert [" + Mapping.DestinationTable.SqlSchema + "].[" +
                                                Mapping.DestinationTable.Name + "] OFF;";
                        }
                    }
                }
                SqlCommand.CommandText = sqlUpdateInsert;
                if (SqlCommand.Connection.State != ConnectionState.Open)
                {
                    SqlCommand.Connection.Open();
                }
                SqlCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw GetMoveDataToMainTableException(ex, SqlCommand, Mapping, tempTablePrefix, insertColumns);
            }
        }

        protected static bool HasIdentity(Mapping mapping)
        {
            return mapping.GetColumnMappings().Any(cm => cm.Active && ((SqlColumn)cm.DestinationColumn).IsIdentity);
        }

        protected void SqlBulkCopierWriteToServer()
        {
            try
            {
                if (SkipFailingRows)
                {
                    SkippedFailedRowsCount = 0;
                }
                SqlBulkCopier.WriteToServer(TableToWrite);
            }
            catch
            {
                string errors = BulkCopyHelper.GetBulkCopyFailures(SqlBulkCopier, TableToWrite);
                if (SkipFailingRows)
                {
                    SkippedFailedRowsCount = errors.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries).Length - 1;
                    SkippedFailedRowsCount = SkippedFailedRowsCount < 0 ? 0 : SkippedFailedRowsCount;
                    if (SkippedFailedRowsCount > 0)
                    {
                        logger.Log($"Skipped {SkippedFailedRowsCount} failed rows from the temporary {mapping.DestinationTable.Name} table");
                    }
                }
                else
                {
                    throw new Exception(errors);
                }
            }
        }

        private static string GetWhereConditionForDelete(Mapping mapping)
        {
            StringBuilder ret = new StringBuilder();

            var columnMappings = mapping.GetColumnMappings();
            bool isPrimaryKeyColumnExists = columnMappings.IsKeyColumnExists();

            foreach (ColumnMapping columnMapping in columnMappings)
            {
                if (columnMapping.Active)
                {
                    SqlColumn column = (SqlColumn)columnMapping.DestinationColumn;
                    if (column.IsKeyColumn(columnMappings) || !isPrimaryKeyColumnExists)
                    {
                        ret.AppendFormat("([{0}].[{1}].[{2}]=[{2}] or ([{0}].[{1}].[{2}] is null and [{2}] is null)) and ",
                            mapping.DestinationTable.SqlSchema, mapping.DestinationTable.Name, column.Name);
                    }
                }
            }
            ret.Remove(ret.Length - 4, 4);
            return ret.ToString();
        }

        private static void DeleteExcessFromRelationTables(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            if (mapping.DestinationTable.Name == "EcomProducts")
            {
                DeleteExcessFromProductCategoryFieldValue(mapping, extraConditions, sqlCommand, tempTablePrefix);
                DeleteExcessFromVariantOptionsProductRelation(mapping, extraConditions, sqlCommand, tempTablePrefix);
                DeleteExcessFromAssortmentItems(mapping, extraConditions, sqlCommand, tempTablePrefix);
                DeleteExcessFromAssortmentsProductRelation(mapping, extraConditions, sqlCommand, tempTablePrefix);
            }
        }

        private static void DeleteExcessFromProductCategoryFieldValue(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            try
            {
                StringBuilder sqlClean = new StringBuilder("DELETE pcfv FROM [EcomProductCategoryFieldValue] pcfv INNER JOIN(");

                sqlClean.Append("SELECT ProductID, ProductVariantID, ProductLanguageID FROM [EcomProducts] WHERE ");
                sqlClean.Append("NOT EXISTS (SELECT * FROM [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + tempTablePrefix + "] WHERE ");

                sqlClean.Append(GetWhereConditionForDelete(mapping));
                sqlClean.Append(")");
                if (extraConditions.Length > 0)
                    sqlClean.Append(extraConditions);

                sqlClean.Append(") p ");
                sqlClean.Append("ON pcfv.FieldValueProductId = p.ProductID AND pcfv.FieldValueProductVariantId = p.ProductVariantID AND pcfv.FieldValueProductLanguageId = p.ProductLanguageID");
                sqlCommand.CommandText = sqlClean.ToString();
                sqlCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string msg = "Failed to delete products from EcomProductCategoryFieldValue relation table. ";
                msg += "Exception message: " + ex.Message + " Sql query: " + sqlCommand.CommandText;
                throw new Exception(msg, ex);
            }
        }

        private static void DeleteExcessFromVariantOptionsProductRelation(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            try
            {
                StringBuilder sqlClean = new StringBuilder("DELETE vopr FROM [EcomVariantOptionsProductRelation] vopr INNER JOIN(");

                sqlClean.Append("SELECT ProductID, ProductVariantID FROM [EcomProducts] WHERE ");
                sqlClean.Append("NOT EXISTS (SELECT * FROM [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + tempTablePrefix + "] WHERE ");

                sqlClean.Append(GetWhereConditionForDelete(mapping));
                sqlClean.Append(")");
                if (extraConditions.Length > 0)
                    sqlClean.Append(extraConditions);

                sqlClean.Append(") p ");
                sqlClean.Append("ON vopr.VariantOptionsProductRelationProductID = p.ProductID AND vopr.VariantOptionsProductRelationVariantID = p.ProductVariantID");
                sqlCommand.CommandText = sqlClean.ToString();
                sqlCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string msg = "Failed to delete rows from EcomVariantOptionsProductRelation relation table. ";
                msg += "Exception message: " + ex.Message + " Sql query: " + sqlCommand.CommandText;
                throw new Exception(msg, ex);
            }
        }

        private static void DeleteExcessFromAssortmentItems(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            try
            {
                StringBuilder sqlClean = new StringBuilder("DELETE ai FROM [EcomAssortmentItems] ai INNER JOIN(");

                sqlClean.Append("SELECT ProductID, ProductVariantID FROM [EcomProducts] WHERE ");
                sqlClean.Append("NOT EXISTS (SELECT * FROM [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + tempTablePrefix + "] WHERE ");

                sqlClean.Append(GetWhereConditionForDelete(mapping));
                sqlClean.Append(")");
                if (extraConditions.Length > 0)
                    sqlClean.Append(extraConditions);

                sqlClean.Append(") p ");
                sqlClean.Append("ON ai.AssortmentItemProductID = p.ProductID AND ai.AssortmentItemProductVariantID = p.ProductVariantID");
                sqlCommand.CommandText = sqlClean.ToString();
                sqlCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string msg = "Failed to delete rows from EcomAssortmentItems table. ";
                msg += "Exception message: " + ex.Message + " Sql query: " + sqlCommand.CommandText;
                throw new Exception(msg, ex);
            }
        }

        private static void DeleteExcessFromAssortmentsProductRelation(Mapping mapping, string extraConditions, SqlCommand sqlCommand, string tempTablePrefix)
        {
            try
            {
                StringBuilder sqlClean = new StringBuilder("DELETE apr FROM [EcomAssortmentProductRelations] apr INNER JOIN(");

                sqlClean.Append("SELECT ProductID, ProductVariantID FROM [EcomProducts] WHERE ");
                sqlClean.Append("NOT EXISTS (SELECT * FROM [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + tempTablePrefix + "] WHERE ");

                sqlClean.Append(GetWhereConditionForDelete(mapping));
                sqlClean.Append(")");
                if (extraConditions.Length > 0)
                    sqlClean.Append(extraConditions);

                sqlClean.Append(") p ");
                sqlClean.Append("ON apr.AssortmentProductRelationProductID = p.ProductID AND apr.AssortmentProductRelationProductVariantID = p.ProductVariantID");
                sqlCommand.CommandText = sqlClean.ToString();
                sqlCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string msg = "Failed to delete rows from EcomAssortmentProductRelations table. ";
                msg += "Exception message: " + ex.Message + " Sql query: " + sqlCommand.CommandText;
                throw new Exception(msg, ex);
            }
        }

        #region SqlExceptionDetails

        public static Exception GetMoveDataToMainTableException(Exception ex, SqlCommand sqlCommand, Mapping mapping, string tempTablePrefix, List<string> insertColumns)
        {
            return GetMoveDataToMainTableException(ex, sqlCommand, mapping, tempTablePrefix, insertColumns, null, null);
        }

        public static Exception GetMoveDataToMainTableException(Exception ex, SqlCommand sqlCommand, Mapping mapping, string tempTablePrefix, List<string> insertColumns,
            string sourceTableName, string destinationTableName)
        {
            string message = string.Format("failed to move data from temporary table [{0}.{1}{2}] to main table [{0}.{3}]. Exception: {4} Sql query: {5}",
                mapping.DestinationTable.SqlSchema,
                string.IsNullOrEmpty(sourceTableName) ? mapping.DestinationTable.Name : sourceTableName,
                tempTablePrefix,
                string.IsNullOrEmpty(destinationTableName) ? mapping.DestinationTable.Name : destinationTableName,
                ex.Message, sqlCommand.CommandText);
            if (ex is SqlException)
            {
                message += GetSqlExceptionMessage(ex as SqlException, sqlCommand, mapping, tempTablePrefix, insertColumns);
            }
            return new Exception(message, ex);
        }

        private class FKTableConstraint
        {
            public string FKColumn;
            public string PKColumn;
            public string PKTable;
        }

        private static List<FKTableConstraint> GetTableConstraints(SqlCommand sqlCommand, string fkTableName)
        {
            List<FKTableConstraint> ret = new List<FKTableConstraint>();

            sqlCommand.CommandText = string.Format(
                    @"SELECT  PK_Table = PK.TABLE_NAME,FK_Column = CU.COLUMN_NAME,PK_Column = PT.COLUMN_NAME, Constraint_Name = C.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME INNER JOIN ( SELECT i1.TABLE_NAME, i2.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1 INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY' ) PT ON PT.TABLE_NAME = PK.TABLE_NAME 
                    WHERE FK.TABLE_NAME = '{0}'", fkTableName);

            using (SqlDataReader reader = sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    FKTableConstraint row = new FKTableConstraint();
                    row.PKTable = Converter.ToString(reader["PK_Table"]);
                    row.PKColumn = Converter.ToString(reader["PK_Column"]);
                    row.FKColumn = Converter.ToString(reader["FK_Column"]);
                    ret.Add(row);
                }
            }
            return ret;
        }

        private static string GetSqlExceptionMessage(SqlException exception, SqlCommand sqlCommand, Mapping mapping, string tempTablePrefix, List<string> columns)
        {
            StringBuilder message = new StringBuilder();
            //If The INSERT statement conflicted with the FOREIGN KEY constraint
            if (exception.Number == 547)
            {
                var columnMappings = mapping.GetColumnMappings();
                List<ColumnMapping> mappings = columnMappings.Where(cm => cm.Active && ((SqlColumn)cm.DestinationColumn).IsKeyColumn(columnMappings)).ToList();
                if (mappings.Count > 0)
                {
                    List<FKTableConstraint> constraints = GetTableConstraints(sqlCommand, mapping.DestinationTable.Name);
                    foreach (FKTableConstraint constraint in constraints)
                    {
                        foreach (ColumnMapping cm in mappings)
                        {
                            if (cm.DestinationColumn != null && string.Compare(cm.DestinationColumn.Name, constraint.FKColumn, true) == 0)
                            {
                                sqlCommand.CommandText = string.Format("select {0} from [{1}].[{2}] where [{3}] not in (select distinct [{4}] from [{1}].[{5}])",
                                    string.Join(",", columns),
                                    mapping.DestinationTable.SqlSchema, mapping.DestinationTable.Name + tempTablePrefix,
                                    constraint.FKColumn, constraint.PKColumn, constraint.PKTable);
                                using (SqlDataReader reader = sqlCommand.ExecuteReader())
                                {
                                    if (reader.HasRows)
                                    {
                                        message.Append(System.Environment.NewLine + "Failed rows:" + System.Environment.NewLine);
                                        foreach (string column in columns)
                                        {
                                            message.AppendFormat(" [{0}],", column.Trim(new char[] { '[', ']' }));
                                        }
                                        message.Replace(",", "", message.Length - 1, 1);
                                        message.Append(System.Environment.NewLine);
                                        while (reader.Read())
                                        {
                                            foreach (string column in columns)
                                            {
                                                message.AppendFormat(" [{0}],", reader[column.Trim(new char[] { '[', ']' })]);
                                            }
                                            message.Replace(",", "", message.Length - 1, 1);
                                            message.Append(System.Environment.NewLine);
                                        }
                                        message.Replace(System.Environment.NewLine, "", message.Length - 2, 2);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return message.ToString();
        }
        #endregion SqlExceptionDetails

        #region IDisposable Implementation
        protected bool Disposed;

        protected virtual void Dispose(bool disposing)
        {
            lock (this)
            {
                // Do nothing if the object has already been disposed of.
                if (Disposed)
                    return;

                if (disposing)
                {
                    // Release diposable objects used by this instance here.

                    if (DataToWrite != null)
                        DataToWrite.Dispose();
                    if (TableToWrite != null)
                        TableToWrite.Dispose();
                    if (SqlCommand != null)
                        SqlCommand.Dispose();
                }

                // Release unmanaged resources here. Don't access reference type fields.

                // Remember that the object has been disposed of.
                Disposed = true;
            }
        }

        public virtual void Dispose()
        {
            Dispose(true);
            // Unregister object for finalization.
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}