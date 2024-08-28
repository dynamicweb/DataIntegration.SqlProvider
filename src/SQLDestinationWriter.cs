using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{
    /// <summary>
    /// Sql destination writer
    /// </summary>
    public class SqlDestinationWriter : BaseSqlWriter, IDestinationWriter, IDisposable
    {
        public SqlCommand SqlCommand;

        public new Mapping Mapping { get; }

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
        private readonly ColumnMappingCollection _columnMappings;

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
            Mapping = mapping;
            _columnMappings = new ColumnMappingCollection(ReplaceKeyColumnsWithAutoIdIfExists(Mapping));
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
            Mapping = mapping;
            _columnMappings = new ColumnMappingCollection(ReplaceKeyColumnsWithAutoIdIfExists(Mapping));
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
            Mapping = mapping;
            _columnMappings = new ColumnMappingCollection(ReplaceKeyColumnsWithAutoIdIfExists(Mapping));
            SqlCommand = mockSqlCommand;
            this.removeMissingAfterImport = removeMissingAfterImport;
            this.logger = logger;
            this.tempTablePrefix = tempTablePrefix;
            this.discardDuplicates = discardDuplicates;
            Initialize();
        }

        protected new virtual void Initialize()
        {
            List<SqlColumn> destColumns = new List<SqlColumn>();
            foreach (ColumnMapping columnMapping in _columnMappings.DistinctBy(obj => obj.DestinationColumn.Name))
            {
                destColumns.Add((SqlColumn)columnMapping.DestinationColumn);
            }
            if (Mapping.DestinationTable != null && Mapping.DestinationTable.Name == "EcomAssortmentPermissions")
            {
                if (_columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAccessUserID", true) == 0) == null)
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

        private static IEnumerable<ColumnMapping> ReplaceKeyColumnsWithAutoIdIfExists(Mapping mapping)
        {
            //will move this to MappingExtensions - US https://dev.azure.com/dynamicwebsoftware/Dynamicweb/_workitems/edit/20900
            if (mapping == null) return [];

            var autoIdDestinationColumnName = MappingExtensions.GetAutoIdColumnName(mapping.DestinationTable?.Name ?? "");
            if (string.IsNullOrEmpty(autoIdDestinationColumnName)) return mapping.GetColumnMappings();

            var columnMappings = mapping.GetColumnMappings().ToList();
            var autoIdColumnMapping = columnMappings.Where(obj => obj.DestinationColumn.Name.Equals(autoIdDestinationColumnName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            if (autoIdColumnMapping != null)
            {
                columnMappings.ForEach(obj => obj.IsKey = false);
                autoIdColumnMapping.IsKey = true;
            }
            return columnMappings;
        }

        /// <summary>
        /// Writes the specified row.
        /// </summary>
        /// <param name="Row">The row to be written.</param>
        public new virtual void Write(Dictionary<string, object> row)
        {
            if (!Mapping.Conditionals.CheckConditionals(row))
            {
                return;
            }

            DataRow dataRow = TableToWrite.NewRow();

            var columnMappings = _columnMappings.Where(cm => cm.Active);
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
                    throw new Exception(GetRowValueNotFoundMessage(row, columnMapping.SourceColumn.Table.Name, columnMapping.SourceColumn.Name));
                }
            }

            if (!discardDuplicates || !duplicateRowsHandler.IsRowDuplicate(columnMappings, Mapping, dataRow, row))
            {
                TableToWrite.Rows.Add(dataRow);

                if (TableToWrite.Rows.Count >= 1000)
                {
                    rowsToWriteCount = rowsToWriteCount + TableToWrite.Rows.Count;
                    SkippedFailedRowsCount = SqlBulkCopierWriteToServer(SqlBulkCopier, TableToWrite, SkipFailingRows, Mapping, logger);
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
            DeleteRowsNotInSourceFromMainTable(extraConditions);
        }

        /// <summary>
        /// Deletes rows not present in the import source
        /// </summary>
        /// <param name="extraConditions">Where condition to filter data for deletion</param>
        public virtual long DeleteRowsNotInSourceFromMainTable(string extraConditions)
        {
            if (removeMissingAfterImport || removeMissingAfterImportDestinationTablesOnly)
            {
                return DeleteExcessFromMainTable(SqlCommand, Mapping, extraConditions, tempTablePrefix, removeMissingAfterImportDestinationTablesOnly);
            }
            return 0;
        }

        /// <summary>
        /// Write data using SQL bulk copier
        /// </summary>
        public virtual void FinishWriting()
        {
            SkippedFailedRowsCount = SqlBulkCopierWriteToServer(SqlBulkCopier, TableToWrite, SkipFailingRows, Mapping, logger);
            if (TableToWrite.Rows.Count != 0)
            {
                rowsToWriteCount = rowsToWriteCount + TableToWrite.Rows.Count - SkippedFailedRowsCount;
                logger.Log("Added " + rowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
            }
        }

        /// <summary>
        /// Close writer
        /// </summary>
        public new virtual void Close()
        {
            string tableName = Mapping.DestinationTable.Name + tempTablePrefix;
            SqlCommand.CommandText = $"if exists (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{tableName}') AND type in (N'U')) drop table [{tableName}]";
            SqlCommand.ExecuteNonQuery();
            ((IDisposable)SqlBulkCopier).Dispose();
            if (duplicateRowsHandler != null)
            {
                duplicateRowsHandler.Dispose();
            }
        }

        protected internal int MoveDataToMainTable(SqlTransaction sqlTransaction)
        {
            return MoveDataToMainTable(SqlCommand, Mapping, sqlTransaction, tempTablePrefix);
        }

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