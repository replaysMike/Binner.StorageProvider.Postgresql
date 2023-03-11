using System.Collections.Generic;

namespace Binner.StorageProvider.Postgresql
{
    internal class PostgresqlStorageConfiguration
    {
        public string ConnectionString { get; set; } = string.Empty;

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
