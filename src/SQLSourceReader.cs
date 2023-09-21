using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{
    public class SqlSourceReader : ISourceReader, IResponseWriter
    {
        protected SqlCommand _command;
        protected SqlDataReader _reader;
        protected Mapping mapping;

        private SqlCommand _responseCommand;
        private int _responseMappingCounter = 0;
        private List<KeyValuePair<string, Dictionary<string, object>>> _responseSQLs = new List<KeyValuePair<string, Dictionary<string, object>>>();
        private List<string> _sourceColumnNamesWithIsKeySet = new List<string>();

        public SqlSourceReader(Mapping mapping, SqlConnection connection)
        {
            DoInitialization(mapping, connection);

            if (connection.State.ToString() != "Open")
                connection.Open();
            LoadReaderFromDatabase();
        }

        protected void DoInitialization(Mapping mapping, SqlConnection connection)
        {
            this.mapping = mapping;
            _command = new SqlCommand { Connection = connection };
            _responseCommand = new SqlCommand { Connection = connection };
            _sourceColumnNamesWithIsKeySet = mapping.GetColumnMappings().Where(obj => obj.SourceColumn != null && obj.IsKey).Select(obj => obj.SourceColumn.Name).Distinct().ToList();

            int _commandtimeout = Dynamicweb.Configuration.SystemConfiguration.Instance.Contains("/Globalsettings/Settings/DataIntegration/SQLSourceCommandTimeout") ?
                Converter.ToInt32(Dynamicweb.Configuration.SystemConfiguration.Instance.GetValue("/Globalsettings/Settings/DataIntegration/SQLSourceCommandTimeout")) :
                Converter.ToInt32(Dynamicweb.Configuration.SystemConfiguration.Instance.GetValue("/Globalsettings/DataIntegration/SQLSourceCommandTimeout"));
            if (_commandtimeout > 0)
            {
                _command.CommandTimeout = _commandtimeout;
                _responseCommand.CommandTimeout = _commandtimeout;
            }
        }

        private void LoadReaderFromDatabase()
        {
            try
            {
                ColumnMappingCollection columnmappings = mapping.GetColumnMappings();
                if (columnmappings.Count == 0)
                    return;
                string columns = GetColumns();
                string fromTables = GetFromTables();
                string sql = "select * from (select " + columns + " from  " + fromTables + ") as result";

                List<SqlParameter> parameters = new List<SqlParameter>();
                string conditionalsSql = MappingExtensions.GetConditionalsSql(out parameters, mapping.Conditionals, false, false);
                if (conditionalsSql != "")
                {
                    conditionalsSql = conditionalsSql.Substring(0, conditionalsSql.Length - 4);
                    sql = sql + " where " + conditionalsSql;
                    foreach (SqlParameter p in parameters)
                        _command.Parameters.Add(p);
                }
                _command.CommandText = sql;
                _reader = _command.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to open sqlSourceReader. Reason: " + ex.Message, ex);
            }
        }

        protected virtual string GetFromTables()
        {
            string result = "[" + mapping.SourceTable.SqlSchema + "].[" + mapping.SourceTable.Name + "]";
            if (mapping.SourceTable != null && mapping.SourceTable.Name == "EcomAssortmentPermissions" &&
                (mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && cm.SourceColumn.Name.ToLower() == "AssortmentPermissionAccessUserID".ToLower()) != null ||
                mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && cm.SourceColumn.Name.ToLower() == "AssortmentPermissionCustomerNumber".ToLower()) != null ||
                mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && cm.SourceColumn.Name.ToLower() == "AssortmentPermissionExternalID".ToLower()) != null))
            {
                result = "[" + mapping.SourceTable.SqlSchema + "].[" + mapping.SourceTable.Name + "] as outer" + mapping.SourceTable.Name;
                result = result + " join AccessUser on AssortmentPermissionAccessUserID=AccessUserID";
            }
            return result;
        }

        protected string GetDistinctColumnsFromMapping()
        {
            return GetDistinctColumnsFromMapping(new string[] { });
        }

        protected string GetDistinctColumnsFromMapping(string[] columnsToSkip)
        {
            if (columnsToSkip.Length > 0)
            {
                return mapping.GetColumnMappings().Where(fm => fm.SourceColumn != null && !columnsToSkip.Any(cts => string.Compare(cts, fm.SourceColumn.Name, true) == 0)).GroupBy(m => new { m.SourceColumn.Name }).Select(g => g.First())
                    .Aggregate("", (current, fm) => current + "[" + fm.SourceColumn.Name + "], ");
            }
            else
            {
                return mapping.GetColumnMappings().Where(fm => fm.SourceColumn != null).GroupBy(m => new { m.SourceColumn.Name }).Select(g => g.First())
                    .Aggregate("", (current, fm) => current + "[" + fm.SourceColumn.Name + "], ");
            }
        }

        protected virtual string GetColumns()
        {
            string columns = GetDistinctColumnsFromMapping();

            if (mapping.SourceTable != null && mapping.SourceTable.Name == "EcomAssortmentPermissions")
            {
                columns = GetDistinctColumnsFromMapping(new string[] { "AssortmentPermissionCustomerNumber", "AssortmentPermissionExternalID" });
                if (mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && cm.SourceColumn.Name == "AssortmentPermissionCustomerNumber") != null)
                {
                    columns = columns + "(SELECT AccessUserCustomerNumber FROM AccessUser JOIN EcomAssortmentPermissions on AssortmentPermissionAccessUserID=AccessUserID WHERE AccessUserID=outerEcomAssortmentPermissions.AssortmentPermissionAccessUserID) as AssortmentPermissionCustomerNumber, ";
                }
                if (mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && string.Equals(cm.SourceColumn.Name, "AssortmentPermissionExternalID", System.StringComparison.OrdinalIgnoreCase)) != null)
                {
                    columns = columns + "(SELECT AccessUserExternalID FROM AccessUser JOIN EcomAssortmentPermissions on AssortmentPermissionAccessUserID=AccessUserID WHERE AccessUserID=outerEcomAssortmentPermissions.AssortmentPermissionAccessUserID) as AssortmentPermissionExternalID, ";
                }
            }

            columns += GetColumnsFromMappingConditions();

            columns = columns.Substring(0, columns.Length - 2);
            return columns;
        }

        protected string GetColumnsFromMappingConditions()
        {
            return GetColumnsFromMappingConditions(new string[] { });
        }

        protected string GetColumnsFromMappingConditions(string[] columnsToSkip)
        {
            string ret = string.Empty;
            if (mapping.Conditionals.Count > 0)
            {
                List<ColumnMapping> columnMappings = mapping.GetColumnMappings().Where(cm => cm.SourceColumn != null).ToList();
                foreach (MappingConditional mc in mapping.Conditionals.Where(mc => mc != null && mc.SourceColumn != null).GroupBy(g => new { g.SourceColumn.Name }).Select(g => g.First()))
                {
                    if (!columnsToSkip.Any(cts => string.Compare(cts, mc.SourceColumn.Name, true) == 0) && !columnMappings.Any(cm => string.Compare(cm.SourceColumn.Name, mc.SourceColumn.Name, true) == 0))
                    {
                        ret += "[" + mc.SourceColumn.Name + "], ";
                    }
                }
            }
            return ret;
        }

        protected bool IsColumnUsedInMappingConditions(string columnName)
        {
            return mapping.Conditionals.Where(mc => mc != null && mc.SourceColumn != null).Any(mc => string.Compare(mc.SourceColumn.Name, columnName, true) == 0);
        }

        protected SqlSourceReader()
        {
        }

        public virtual bool IsDone()
        {
            if (_reader.Read())
                return false;
            _reader.Close();
            return true;
        }

        /// <summary>
        /// base implementation, 
        /// </summary>
        /// <returns></returns>
        public virtual Dictionary<string, object> GetNext()
        {
            return mapping.GetColumnMappings().Where(columnMapping => columnMapping.SourceColumn != null).GroupBy(cm => cm.SourceColumn.Name, (key, group) => group.First()).ToDictionary(columnMapping => columnMapping.SourceColumn.Name, columnMapping => _reader[columnMapping.SourceColumn.Name]);
        }

        public void Dispose()
        {
            _reader.Close();
        }

        public void Write(Dictionary<string, object> row)
        {
            if (row != null && row.Any())
            {
                var columnMapping = mapping.GetResponseColumnMappings().FirstOrDefault();
                if (columnMapping != null && columnMapping.DestinationColumn?.Table != null)
                {
                    if (_sourceColumnNamesWithIsKeySet is null || _sourceColumnNamesWithIsKeySet.Count() == 0)
                    {
                        throw new Exception($"The mapping {columnMapping.SourceColumn?.Table?.Name} - {columnMapping.DestinationColumn?.Table?.Name} must have at least one Key Column set");
                    }
                    string keyColumnPrefix = "_keyColumn";
                    var destinationTable = columnMapping.DestinationColumn.Table;
                    foreach (KeyValuePair<string, object> item in row)
                    {
                        var sqlParams = new Dictionary<string, object>
                        {
                            { $"{item.Key}{_responseMappingCounter}", item.Value }
                        };

                        string whereStatement = "";
                        foreach (var keyColumn in _sourceColumnNamesWithIsKeySet)
                        {
                            whereStatement += $"{keyColumn} = @{keyColumnPrefix}{keyColumn}{_responseMappingCounter} AND ";
                            sqlParams.Add($"{keyColumnPrefix}{keyColumn}{_responseMappingCounter}", _reader[keyColumn]);
                        }
                        whereStatement = $"WHERE {whereStatement.Substring(0, whereStatement.Length - 4)}";

                        _responseSQLs.Add(new KeyValuePair<string, Dictionary<string, object>>($"UPDATE [{destinationTable.SqlSchema}].[{destinationTable.Name}] SET {item.Key} = @{item.Key}{_responseMappingCounter} {whereStatement};", sqlParams));
                        _responseMappingCounter++;
                    }
                }
            }
        }

        public void Close()
        {
            if (_responseSQLs.Any())
            {
                if (_responseCommand.Connection.State != ConnectionState.Open)
                {
                    _responseCommand.Connection.Open();
                }
                int chunkSize = 100;
                var taken = 0;
                while (taken < _responseSQLs.Count)
                {
                    var chunk = _responseSQLs.Skip(taken).Take(chunkSize);
                    taken = taken + chunkSize;
                    _responseCommand.CommandText = "";
                    _responseCommand.Parameters.Clear();
                    foreach (KeyValuePair<string, Dictionary<string, object>> pair in chunk)
                    {
                        _responseCommand.CommandText += pair.Key;
                        foreach (var param in pair.Value)
                        {
                            _responseCommand.Parameters.Add(new SqlParameter(param.Key, param.Value));
                        }
                    }
                    _responseCommand.ExecuteNonQuery();
                }
                _responseCommand.Connection.Close();
            }
        }
    }
}
