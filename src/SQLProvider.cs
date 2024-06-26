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
namespace Dynamicweb.DataIntegration.Providers.SqlProvider;

[AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("SQL Provider"), AddInDescription("SQL provider"), AddInIgnore(false)]
public class SqlProvider : BaseSqlProvider, ISource, IDestination
{
    private Schema Schema;
    [AddInParameter("Source server"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
    public string SourceServer
    {
        get { return Server; }
        set { Server = value; }
    }
    [AddInParameter("Destination server"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
    public string DestinationServer
    {
        get { return Server; }
        set { Server = value; }
    }
    [AddInParameter("Use integrated security to connect to source server"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
    public bool SourceServerSSPI
    {
        get;
        set;
    }
    [AddInParameter("Use integrated security to connect to destination server"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
    public bool DestinationServerSSPI
    {
        get;
        set;
    }
    [AddInParameter("Sql source server username"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
    public string SourceUsername
    {
        get { return Username; }
        set { Username = value; }
    }
    [AddInParameter("Sql destination server username"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
    public string DestinationUsername
    {
        get { return Username; }
        set { Username = value; }
    }
    [AddInParameter("Sql source server password"), AddInParameterEditor(typeof(TextParameterEditor), "password=true"), AddInParameterGroup("Source")]
    public string SourcePassword
    {
        get { return Password; }
        set { Password = value; }
    }
    [AddInParameter("Sql destination server password"), AddInParameterEditor(typeof(TextParameterEditor), "password=true"), AddInParameterGroup("Destination")]
    public string DestinationPassword
    {
        get { return Password; }
        set { Password = value; }
    }
    [AddInParameter("Sql source database"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
    public string SourceDatabase
    {
        get { return Catalog; }
        set { Catalog = value; }
    }
    [AddInParameter("Sql destination database"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
    public string DestinationDatabase
    {
        get { return Catalog; }
        set { Catalog = value; }
    }
    [AddInParameter("Sql source connection string"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Source")]
    public string SourceConnectionString
    {
        get { return ManualConnectionString; }
        set { ManualConnectionString = value; }
    }
    [AddInParameter("Sql destination connection string"), AddInParameterEditor(typeof(TextParameterEditor), ""), AddInParameterGroup("Destination")]
    public string DestinationConnectionString
    {
        get { return ManualConnectionString; }
        set { ManualConnectionString = value; }
    }

    [AddInParameter("Remove missing rows after import"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Removes rows from the destination and relation tables. This option takes precedence"), AddInParameterGroup("Destination")]
    public bool RemoveMissingAfterImport
    {
        get;
        set;
    }

    [AddInParameter("Remove missing rows after import in the destination tables only"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes rows not present in the import source - excluding related tabled"), AddInParameterGroup("Destination"), AddInParameterOrder(35)]
    public bool RemoveMissingAfterImportDestinationTablesOnly
    {
        get;
        set;
    }

    [AddInParameter("Discard duplicates"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When ON, duplicate rows are skipped"), AddInParameterGroup("Destination")]
    public virtual bool DiscardDuplicates { get; set; }

    [AddInParameter("Persist successful rows and skip failing rows"), AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Checking this box allows the activity to do partial imports by skipping problematic records and keeping the succesful ones"), AddInParameterGroup("Destination"), AddInParameterOrder(100)]
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
    protected SqlConnection Connection
    {
        get
        {
            connection ??= new SqlConnection(SqlConnectionString);
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

    public override void LoadSettings(Job job)
    {
        base.LoadSettings(job);
        if (job.Mappings != null)
        {
            string error = "";
            foreach (var mapping in job.Mappings)
            {
                if (mapping != null && mapping.SourceTable != null && mapping.DestinationTable != null && mapping.Active)
                {
                    var responseColumnMappings = mapping.GetResponseColumnMappings();
                    if (responseColumnMappings != null && responseColumnMappings.Any())
                    {
                        if (!mapping.GetColumnMappings().Where(obj => obj.SourceColumn != null && obj.IsKey).Select(obj => obj.SourceColumn.Name).Any())
                        {
                            error += $"Response mapping for the source table: {mapping.SourceTable?.Name} and destination table: {mapping.DestinationTable?.Name} mapping must have at least one Key column set. ";
                        }
                    }
                }
            }
            if (!string.IsNullOrEmpty(error))
                throw new Exception(error);
        }
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
        return GetSqlSourceSchema(Connection, null);
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
        Schema ??= GetOriginalSourceSchema();
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
                            if (ProcessInputRow(sourceRow, mapping))
                            {
                                writer.Write(sourceRow);
                            }
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
                    int rowsAffected = writer.MoveDataToMainTable(Transaction);
                    if (rowsAffected > 0)
                    {
                        Logger.Log($"The number of rows affected: {rowsAffected} in the {writer.Mapping.DestinationTable.Name} table");
                        writer.RowsAffected += rowsAffected;
                    }
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
                    long rowsAffected = writer.DeleteRowsNotInSourceFromMainTable("");
                    System.Diagnostics.Debug.WriteLine(DateTime.Now + ": excess data Removed from table: " + writer.Mapping.DestinationTable.Name);
                    if (rowsAffected > 0)
                    {
                        Logger.Log($"The number of deleted rows: {rowsAffected} for the destination {writer.Mapping.DestinationTable.Name} table mapping");
                        writer.RowsAffected += (int)rowsAffected;
                    }
                }
            }
            CommitTransaction();
            Logger.Log("Cleanup done");
        }
        catch (Exception ex)
        {
            string msg = ex.Message;
            string stackTrace = ex.StackTrace;

            Logger?.Error($"Error: {msg.Replace(System.Environment.NewLine, " ")} Stack: {stackTrace.Replace(System.Environment.NewLine, " ")}", ex);
            LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {msg} Stack: {stackTrace}", ex);

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
