﻿using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{
    public class SqlSourceReader : BaseSqlReader, ISourceReader, IResponseWriter
    {
        protected SqlSourceReader() : base()
        {
        }

        public SqlSourceReader(Mapping mapping, SqlConnection connection) : base(mapping, connection)
        {
            LoadReaderFromDatabase();
        }

        public void LoadReaderFromDatabase()
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
    }
}
