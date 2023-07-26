using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Xml;
using System.Xml.Linq;

[assembly: InternalsVisibleTo("Dynamicweb.Tests.Integration")]
namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{
    [AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("SQL Provider"), AddInDescription("SQL provider"), AddInIgnore(false)]
    public class SqlProvider : BaseProvider, ISource, IDestination
    {
        protected Schema Schema;

        [AddInParameter("Source server"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual string SourceServer
        {
            get { return Server; }
            set { Server = value; }
        }
        [AddInParameter("Destination server"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual string DestinationServer
        {
            get { return Server; }
            set { Server = value; }
        }
        [AddInParameter("Use integrated security to connect to source server"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual bool SourceServerSSPI
        {
            get;
            set;
        }
        [AddInParameter("Use integrated security to connect to destination server"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual bool DestinationServerSSPI
        {
            get;
            set;
        }
        [AddInParameter("Sql source server username"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual string SourceUsername
        {
            get { return Username; }
            set { Username = value; }
        }
        [AddInParameter("Sql destination server username"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual string DestinationUsername
        {
            get { return Username; }
            set { Username = value; }
        }
        [AddInParameter("Sql source server password"), AddInParameterEditor(typeof(TextParameterEditor), "password=true"), AddInParameterGroup("Source")]
        public virtual string SourcePassword
        {
            get { return Password; }
            set { Password = value; }
        }
        [AddInParameter("Sql destination server password"), AddInParameterEditor(typeof(TextParameterEditor), "password=true"), AddInParameterGroup("Destination")]
        public virtual string DestinationPassword
        {
            get { return Password; }
            set { Password = value; }
        }
        [AddInParameter("Sql source database"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual string SourceDatabase
        {
            get { return Catalog; }
            set { Catalog = value; }
        }
        [AddInParameter("Sql destination database"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual string DestinationDatabase
        {
            get { return Catalog; }
            set { Catalog = value; }
        }
        [AddInParameter("Sql source connection string"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual string SourceConnectionString
        {
            get { return ManualConnectionString; }
            set { ManualConnectionString = value; }
        }
        [AddInParameter("Sql destination connection string"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual string DestinationConnectionString
        {
            get { return ManualConnectionString; }
            set { ManualConnectionString = value; }
        }

        [AddInParameter("Remove missing rows after import"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Removes rows from the destination and relation tables. This option takes precedence"), AddInParameterGroup("Destination")]
        public virtual bool RemoveMissingAfterImport
        {
            get;
            set;
        }

        [AddInParameter("Remove missing rows after import in the destination tables only"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination"), AddInParameterOrder(35)]
        public virtual bool RemoveMissingAfterImportDestinationTablesOnly
        {
            get;
            set;
        }

        [AddInParameter("Discard duplicates"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual bool DiscardDuplicates { get; set; }

        [AddInParameter("Persist successful rows and skip failing rows"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination"), AddInParameterOrder(100)]
        public virtual bool SkipFailingRows { get; set; }

        private string _sqlConnectionString;
        protected string SqlConnectionString
        {
            get
            {
                if (!string.IsNullOrEmpty(_sqlConnectionString))
                    return _sqlConnectionString;

                if (!string.IsNullOrEmpty(ManualConnectionString))
                    return ManualConnectionString;

                //else return constructed connectionString;
                if (string.IsNullOrEmpty(Server) || (!(SourceServerSSPI | DestinationServerSSPI) && (string.IsNullOrEmpty(Username) || string.IsNullOrEmpty(Password))))
                {
                    return "";
                }
                return "Data Source=" + Server + ";Initial Catalog=" + Catalog + (SourceServerSSPI | DestinationServerSSPI ? ";Integrated Security=SSPI" : string.Empty) + ";User Id=" + Username + ";Password=" + Password + ";";
            }
            set { _sqlConnectionString = value; }
        }

        protected SqlConnection connection;
        protected virtual SqlConnection Connection
        {
            get
            {
                if (connection == null)
                    connection = new SqlConnection(SqlConnectionString);
                return connection;
            }
            set { connection = value; }
        }

        private SqlTransaction _transaction;
        protected string Server;
        protected string Username;
        protected string Password;
        protected string Catalog;
        protected string ManualConnectionString;

        public SqlTransaction Transaction
        {
            get { return _transaction ?? (_transaction = Connection.BeginTransaction("SQLProviderTransaction")); }
        }
        public SqlProvider()
        {
        }

        public SqlProvider(string connectionString)
        {
            RemoveMissingAfterImport = false;
            ManualConnectionString = SqlConnectionString = connectionString;
            connection = new SqlConnection(SqlConnectionString);
            DiscardDuplicates = false;
            RemoveMissingAfterImportDestinationTablesOnly = false;
        }

        public SqlProvider(XmlNode xmlNode)
        {
            RemoveMissingAfterImport = false;
            DiscardDuplicates = false;
            RemoveMissingAfterImportDestinationTablesOnly = false;

            foreach (XmlNode node in xmlNode.ChildNodes)
            {
                switch (node.Name)
                {
                    case "SqlConnectionString":
                        {
                            SqlConnectionString = node.FirstChild.Value;
                        }
                        break;
                    case "ManualConnectionString":
                        if (node.HasChildNodes)
                        {
                            ManualConnectionString = node.FirstChild.Value;
                        }
                        break;
                    case "Username":
                        if (node.HasChildNodes)
                        {
                            Username = node.FirstChild.Value;
                        }
                        break;
                    case "Password":
                        if (node.HasChildNodes)
                        {
                            Password = node.FirstChild.Value;
                        }
                        break;
                    case "Server":
                        if (node.HasChildNodes)
                        {
                            Server = node.FirstChild.Value;
                        }
                        break;
                    case "SourceServerSSPI":
                        if (node.HasChildNodes)
                        {
                            SourceServerSSPI = node.FirstChild.Value == "True";
                        }
                        break;
                    case "DestinationServerSSPI":
                        if (node.HasChildNodes)
                        {
                            DestinationServerSSPI = node.FirstChild.Value == "True";
                        }
                        break;
                    case "Catalog":
                        if (node.HasChildNodes)
                        {
                            Catalog = node.FirstChild.Value;
                        }
                        break;
                    case "Schema":
                        Schema = new Schema(node);
                        break;
                    case "RemoveMissingAfterImport":
                        if (node.HasChildNodes)
                        {
                            RemoveMissingAfterImport = node.FirstChild.Value == "True";
                        }
                        break;
                    case "RemoveMissingAfterImportDestinationTablesOnly":
                        if (node.HasChildNodes)
                        {
                            RemoveMissingAfterImportDestinationTablesOnly = node.FirstChild.Value == "True";
                        }
                        break;
                    case "DiscardDuplicates":
                        if (node.HasChildNodes)
                        {
                            DiscardDuplicates = node.FirstChild.Value == "True";
                        }
                        break;
                    case "SkipFailingRows":
                        if (node.HasChildNodes)
                        {
                            SkipFailingRows = node.FirstChild.Value == "True";
                        }
                        break;
                }
            }
            connection = new SqlConnection(SqlConnectionString);
        }

        public override string ValidateDestinationSettings()
        {
            try
            {
                using (
                SqlConnection connection = new SqlConnection(SqlConnectionString))
                {
                    connection.Open();
                    connection.Close();
                    connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                return string.Format("Failed opening database using ConnectionString [{0}]: {1}", SqlConnectionString, ex.Message);
            }
            return "";
        }
        public override string ValidateSourceSettings()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(SqlConnectionString))
                {
                    connection.Open();
                    connection.Close();
                    connection.Dispose();
                }
            }
            catch (Exception)
            {
                return "unable to open a connection to a database using connectionstring: \"" + SqlConnectionString + "\"";
            }

            return null;
        }

        public override void UpdateSourceSettings(ISource source)
        {
            SqlProvider newProvider = (SqlProvider)source;
            SqlConnectionString = newProvider.SqlConnectionString;
            ManualConnectionString = newProvider.ManualConnectionString;
            RemoveMissingAfterImport = newProvider.RemoveMissingAfterImport;
            RemoveMissingAfterImportDestinationTablesOnly = newProvider.RemoveMissingAfterImportDestinationTablesOnly;
            Username = newProvider.Username;
            Password = newProvider.Password;
            Server = newProvider.Server;
            SourceServerSSPI = newProvider.SourceServerSSPI;
            DestinationServerSSPI = newProvider.DestinationServerSSPI;
            Catalog = newProvider.Catalog;
            DiscardDuplicates = newProvider.DiscardDuplicates;
            SkipFailingRows = newProvider.SkipFailingRows;
        }

        public override void UpdateDestinationSettings(IDestination destination)
        {
            ISource newProvider = (ISource)destination;
            UpdateSourceSettings(newProvider);
        }

        //Required for addin-compatability
        public override string Serialize()
        {
            XDocument document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));

            XElement root = new XElement("Parameters");
            document.Add(root);

            root.Add(CreateParameterNode(GetType(), "Connection string", SqlConnectionString));
            root.Add(CreateParameterNode(GetType(), "Sql source connection string", ManualConnectionString));
            root.Add(CreateParameterNode(GetType(), "Sql source destination string", ManualConnectionString));
            root.Add(CreateParameterNode(GetType(), "Sql source server username", Username));
            root.Add(CreateParameterNode(GetType(), "Sql destination server username", Username));
            root.Add(CreateParameterNode(GetType(), "Sql source server password", Password));
            root.Add(CreateParameterNode(GetType(), "Sql destination server password", Password));
            root.Add(CreateParameterNode(GetType(), "Source server", Server));
            root.Add(CreateParameterNode(GetType(), "Use integrated security to connect to source server", SourceServerSSPI.ToString()));
            root.Add(CreateParameterNode(GetType(), "Destination server", Server));
            root.Add(CreateParameterNode(GetType(), "Use integrated security to connect to destination server", DestinationServerSSPI.ToString()));
            root.Add(CreateParameterNode(GetType(), "Sql source database", Catalog));
            root.Add(CreateParameterNode(GetType(), "Sql destination database", Catalog));
            root.Add(CreateParameterNode(GetType(), "Discard duplicates", DiscardDuplicates.ToString()));
            root.Add(CreateParameterNode(GetType(), "Remove missing rows after import", RemoveMissingAfterImport.ToString()));
            root.Add(CreateParameterNode(GetType(), "Sql destination connection string", DestinationConnectionString));
            root.Add(CreateParameterNode(GetType(), "Remove missing rows after import in the destination tables only", RemoveMissingAfterImportDestinationTablesOnly.ToString()));
            root.Add(CreateParameterNode(GetType(), "Persist successful rows and skip failing rows", SkipFailingRows.ToString()));

            string ret = document.ToString();
            return ret;
        }

        public new virtual void SaveAsXml(XmlTextWriter xmlTextWriter)
        {
            xmlTextWriter.WriteElementString("RemoveMissingAfterImport", RemoveMissingAfterImport.ToString());
            xmlTextWriter.WriteElementString("RemoveMissingAfterImportDestinationTablesOnly", RemoveMissingAfterImportDestinationTablesOnly.ToString());
            xmlTextWriter.WriteElementString("SqlConnectionString", SqlConnectionString);
            xmlTextWriter.WriteElementString("ManualConnectionString", ManualConnectionString);
            xmlTextWriter.WriteElementString("Username", Username);
            xmlTextWriter.WriteElementString("Password", Password);
            xmlTextWriter.WriteElementString("Server", Server);
            xmlTextWriter.WriteElementString("SourceServerSSPI", SourceServerSSPI.ToString());
            xmlTextWriter.WriteElementString("DestinationServerSSPI", DestinationServerSSPI.ToString());
            xmlTextWriter.WriteElementString("Catalog", Catalog);
            xmlTextWriter.WriteElementString("DiscardDuplicates", DiscardDuplicates.ToString());
            xmlTextWriter.WriteElementString("SkipFailingRows", SkipFailingRows.ToString());

            GetSchema().SaveAsXml(xmlTextWriter);
        }

        public new virtual Schema GetOriginalSourceSchema()
        {
            Schema result = new Schema();
            string sql;

            sql = GetSqlForSchemaBuilding();

            using (var command = new SqlCommand(sql, Connection))
            {
                command.CommandTimeout = 3600;
                if (Connection.State != ConnectionState.Open)
                    Connection.Open();
                Table currentTable = null;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (reader["table_name"].ToString() == "sysdiagrams" || reader["table_name"].ToString() == "dtproperties")
                            continue; //We don't want the system tables.

                        if (currentTable == null)
                            currentTable = result.AddTable(reader["table_name"].ToString(), reader["table_schema"].ToString());
                        string table = reader["table_name"].ToString();
                        string sqlSchema = reader["table_schema"].ToString();
                        string column = reader["column_name"].ToString();
                        if ((currentTable.Name != table) || (currentTable.SqlSchema != sqlSchema))
                            currentTable = result.AddTable(reader["table_name"].ToString(), reader["table_schema"].ToString());
                        int limit = 0;
                        if (!(reader["CHARACTER_MAXIMUM_LENGTH"] is DBNull))
                            limit = (int)reader["CHARACTER_MAXIMUM_LENGTH"];
                        string type = reader[2].ToString();
                        bool isIdentity = reader[4].ToString() == "1";
                        bool isPrimaryKey = reader["IsPrimaryKey"].ToString() == "1";
                        currentTable.AddColumn(new SqlColumn(column, type, currentTable, limit, isIdentity,
                                                               isPrimaryKey));
                    }
                }
                Connection.Close();
            }
            return result;
        }

        protected virtual string GetSqlForSchemaBuilding()
        {
            return "select c.table_name,  c.column_name, Data_type, CHARACTER_MAXIMUM_LENGTH,hasIdentity, c.table_schema," +
                " (SELECT count(*) FROM   sys.objects join INFORMATION_SCHEMA.KEY_COLUMN_USAGE on name=constraint_name WHERE  TYPE = 'PK' and c.TABLE_CATALOG = TABLE_CATALOG AND c.TABLE_SCHEMA = TABLE_SCHEMA AND c.TABLE_NAME = TABLE_NAME AND c.COLUMN_NAME = COLUMN_NAME ) AS IsPrimaryKey " +
                " from INFORMATION_SCHEMA.COLUMNS  c " +
                " left  join (" +
                " SELECT name, OBJECT_NAME(id) as tableName, COLUMNPROPERTY(id, name, 'IsIdentity') as hasIdentity, OBJECTPROPERTY(id,'IsPrimaryKey') as isPrimaryKey FROM syscolumns  WHERE  COLUMNPROPERTY(id, name, 'IsIdentity') !=2) as id " +
                " on c.COLUMN_NAME=name and c.TABLE_NAME=tableName, " +
                " INFORMATION_SCHEMA.TABLES " +
                " where c.TABLE_NAME=INFORMATION_SCHEMA.TABLES.TABLE_NAME order by c.TABLE_NAME,ORDINAL_POSITION";
        }

        public override Schema GetOriginalDestinationSchema()
        {
            return GetOriginalSourceSchema();
        }

        public override void OverwriteSourceSchemaToOriginal()
        {
            Schema = GetOriginalSourceSchema();
        }

        public override void OverwriteDestinationSchemaToOriginal()
        {
            Schema = GetOriginalSourceSchema();
        }

        public new virtual Schema GetSchema()
        {
            if (Schema == null)
            {
                Schema = GetOriginalSourceSchema();
            }
            return Schema;
        }

        public new void Close()
        {
            Connection.Close();
        }


        protected void CommitTransaction()
        {
            if (_transaction != null)
                _transaction.Commit();
            else
                System.Diagnostics.Debug.WriteLine("Tried to commit, but Transaction was null");
            _transaction = null;
        }

        protected void RollbackTransaction()
        {
            if (_transaction != null)
                _transaction.Rollback();
            else
                System.Diagnostics.Debug.WriteLine("Tried to Rollback, but Transaction was null");
            _transaction = null;
        }

        public new ISourceReader GetReader(Mapping mapping)
        {
            return new SqlSourceReader(mapping, Connection);
        }

        private static List<TablePK> GetConstraintList(SqlConnection connection)
        {
            var list = new List<TablePK>();


            using (SqlCommand command = connection.CreateCommand())
            {
                command.CommandText =
                    "SELECT  K_Table = FK.TABLE_NAME, PK_Table = PK.TABLE_NAME,FK_Column = CU.COLUMN_NAME,PK_Column = PT.COLUMN_NAME, Constraint_Name = C.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME INNER JOIN ( SELECT i1.TABLE_NAME, i2.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1 INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY' ) PT ON PT.TABLE_NAME = PK.TABLE_NAME order by K_Table";
                if (connection.State == ConnectionState.Closed)
                    connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var tablepk = list.Find(destTable => destTable.Name == reader[0].ToString());
                    if (tablepk == null)
                    {
                        tablepk = new TablePK(reader[0].ToString());
                        list.Add(tablepk);
                    }
                    if (tablepk.Name != reader[1].ToString())
                        tablepk.TableFK.Add(reader[1].ToString());
                }
                reader.Close();
            }
            return list;

        }
        class TablePK
        {
            public readonly List<string> TableFK = new List<string>();
            public readonly string Name;
            public TablePK(string name)
            {
                Name = name;
            }
        }
        public static void OrderTablesByConstraints(Job job, SqlConnection connection)
        {
            List<TablePK> constraints = GetConstraintList(connection);
            Queue<Table> tables = new Queue<Table>();
            foreach (Mapping map in job.Mappings)
                tables.Enqueue(map.DestinationTable);
            TableCollection orderedTables = new TableCollection();
            int count = tables.Count;
            for (int i = 0; i < count; i++)
            {
                Table table1 = tables.Dequeue();
                if (constraints.Find(destColumn => destColumn.Name == table1.Name) == null)
                    orderedTables.Add(table1);
                else
                    tables.Enqueue(table1);
            }
            int last = 0;
            int infiniteLoopDetector = int.MaxValue;
            while (tables.Count > 0)
            {
                if (tables.Count == last)
                {
                    infiniteLoopDetector = infiniteLoopDetector - 1;
                    if (infiniteLoopDetector < 0)
                    {
                        //log info about the fact that foreign key restraints may not be met
                        orderedTables.AddRange(tables);
                        break;
                    }
                }
                else
                {
                    last = tables.Count;
                    infiniteLoopDetector = last * 10;
                }
                Table table1 = tables.Dequeue();
                var found = constraints.Find(destColumn => destColumn.Name == table1.Name);
                if (found == null)
                    orderedTables.Add(table1);
                else
                {
                    bool foundconstraint = false;
                    if (found.TableFK.Any(fk => constraints.Find(constraint => constraint.Name == fk) != null))
                    {
                        tables.Enqueue(table1);
                        foundconstraint = true;
                    }
                    if (!foundconstraint)
                    {
                        orderedTables.Add(table1);
                        constraints.Remove(found);
                    }
                }
            }
            MappingCollection orderedMappings = new MappingCollection();
            //used for distinction in case when two or more Source tables point to the same Destination table
            List<int> usedIndexes = new List<int>();
            foreach (Table t in orderedTables)
            {
                for (int i = 0; i < job.Mappings.Count(); i++)
                {
                    if (job.Mappings[i].DestinationTable.Name == t.Name && !usedIndexes.Contains(i))
                    {
                        orderedMappings.Add(job.Mappings[i]);
                        usedIndexes.Add(i);
                        break;
                    }
                }
            }
            job.Mappings = orderedMappings;
        }

        public override bool RunJob(Job job)
        {
            ReplaceMappingConditionalsWithValuesFromRequest(job);
            OrderTablesByConstraints(job, Connection);
            List<SqlDestinationWriter> writers = new List<SqlDestinationWriter>();
            Dictionary<string, object> sourceRow = null;
            try
            {
                if (Connection.State != ConnectionState.Open)
                    Connection.Open();

                foreach (Mapping mapping in job.Mappings)
                {
                    if (mapping.Active)
                    {
                        System.Diagnostics.Debug.WriteLine(DateTime.Now + ": moving Data into temp table: " + mapping.DestinationTable.Name);
                        Logger.Log("Starting import of data to table: " + mapping.DestinationTable.Name);
                        using (ISourceReader reader = job.Source.GetReader(mapping))
                        {
                            bool? optionValue = mapping.GetOptionValue("RemoveMissingAfterImport");
                            bool removeMissingAfterImport = optionValue.HasValue ? optionValue.Value : RemoveMissingAfterImport;
                            optionValue = mapping.GetOptionValue("DiscardDuplicates");
                            bool discardDuplicates = optionValue.HasValue ? optionValue.Value : DiscardDuplicates;

                            SqlDestinationWriter writer = new SqlDestinationWriter(mapping, Connection, removeMissingAfterImport, Logger, $"TempTableForSqlProviderImport{mapping.GetId()}", discardDuplicates, RemoveMissingAfterImportDestinationTablesOnly, SkipFailingRows);
                            while (!reader.IsDone())
                            {
                                sourceRow = reader.GetNext();
                                ProcessInputRow(mapping, sourceRow);
                                writer.Write(sourceRow);
                            }
                            writer.FinishWriting();
                            writers.Add(writer);
                        }
                        Logger.Log("Finished import of data to table: " + mapping.DestinationTable.Name);
                        System.Diagnostics.Debug.WriteLine(DateTime.Now + ": Move to table: " + mapping.DestinationTable.Name + " done");
                    }
                }
                sourceRow = null;
                Logger.Log("Import done, doing cleanup");
                foreach (SqlDestinationWriter writer in writers)
                {
                    if (writer.RowsToWriteCount > 0)
                    {
                        System.Diagnostics.Debug.WriteLine(DateTime.Now + ": Moving data to main table: " + writer.Mapping.DestinationTable.Name);
                        writer.MoveDataToMainTable(Transaction);
                    }
                    else
                    {
                        Logger.Log(string.Format("No rows were imported to the table: {0}.", writer.Mapping.DestinationTable.Name));
                    }
                }
                foreach (SqlDestinationWriter writer in Enumerable.Reverse(writers))
                {
                    if (writer.RowsToWriteCount > 0)
                    {
                        System.Diagnostics.Debug.WriteLine(DateTime.Now + ": Removing excess data from table: " + writer.Mapping.DestinationTable.Name);
                        writer.DeleteExcessFromMainTable("");
                        System.Diagnostics.Debug.WriteLine(DateTime.Now + ": excess data Removed from table: " + writer.Mapping.DestinationTable.Name);
                    }
                }
                CommitTransaction();
                Logger.Log("Cleanup done");
            }
            catch (Exception ex)
            {
                string msg = ex.Message;
                LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {ex.Message} Stack: {ex.StackTrace}", ex);

                if (ex.Message.Contains("Subquery returned more than 1 value"))
                    msg += System.Environment.NewLine + "This error usually indicates duplicates on column that is used as primary key or identity.";

                if (ex.Message.Contains("Bulk copy failures"))
                {
                    Logger.Log("Import job failed:");
                    BulkCopyHelper.LogFailedRows(Logger, msg);
                }
                else
                {
                    if (sourceRow != null)
                        msg += GetFailedSourceRowMessage(sourceRow);

                    Logger.Log("Import job failed: " + msg);
                }
                RollbackTransaction();
                return false;
            }
            finally
            {
                foreach (SqlDestinationWriter writer in writers)
                {
                    writer.Close();
                }
                job.Source.Close();
                Connection.Dispose();
                sourceRow = null;
            }
            return true;
        }

        #region ISource Members

        List<SchemaComparerResult> ISource.CheckMapping(Mapping map)
        {
            return new List<SchemaComparerResult>();
        }

        #endregion

        #region IDestination Members

        List<SchemaComparerResult> IDestination.CheckMapping(Mapping map)
        {
            List<SchemaComparerResult> results = new List<SchemaComparerResult>();

            if (map.DestinationTable != null)
            {
                Table dstTable = map.Destination.GetOriginalDestinationSchema().GetTables().Find(t => t.Name == map.DestinationTable.Name);

                if (dstTable != null)
                    results.AddRange(CheckPrimaryKey(map));
            }

            return results;
        }

        private List<SchemaComparerResult> CheckPrimaryKey(Mapping map)
        {
            List<SchemaComparerResult> results = new List<SchemaComparerResult>();
            bool hasKey = false;

            foreach (ColumnMapping cm in map.GetColumnMappings())
            {
                if (cm.DestinationColumn != null && cm.DestinationColumn.IsPrimaryKey)
                {
                    hasKey = true;
                    break;
                }
            }

            if (!hasKey)
                results.Add(new SchemaComparerResult(ProviderType.Destination, SchemaCompareErrorType.NoOnePrimaryKey, string.Format("[Table: {0}]", map.DestinationTable.Name)));

            return results;
        }

        #endregion
    }



}
