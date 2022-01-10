namespace Binner.StorageProvider.Postgresql
{
    internal class PostgresqlStorageConfiguration
    {
        public string ConnectionString { get; set; }

        public PostgresqlStorageConfiguration()
        {
        }

        public PostgresqlStorageConfiguration(IDictionary<string, string> config)
        {
            if (config.ContainsKey("ConnectionString"))
                ConnectionString = config["ConnectionString"];
        }
    }
}
