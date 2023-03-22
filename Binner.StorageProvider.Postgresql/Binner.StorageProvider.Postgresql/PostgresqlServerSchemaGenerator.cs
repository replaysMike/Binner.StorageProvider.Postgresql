using Binner.Model.Common;
using System.ComponentModel.DataAnnotations;
using TypeSupport;
using TypeSupport.Extensions;

namespace Binner.StorageProvider.Postgresql
{
    public class PostgresqlServerSchemaGenerator<T>
    {
        private string _dbName;
        private ICollection<ExtendedProperty> _tables;

        public PostgresqlServerSchemaGenerator(string databaseName)
        {
            _dbName = databaseName;
            var properties = typeof(T).GetProperties(PropertyOptions.HasGetter);
            _tables = properties.Where(x => x.Type.IsCollection).ToList();
        }

        public string CreateDatabaseIfNotExists()
            => $@"CREATE DATABASE {Quote(_dbName)}";

        public string CreateTableSchemaIfNotExists() => $@"CREATE SCHEMA IF NOT EXISTS dbo;
{string.Join("\r\n", GetTableSchemas())}";

        private ICollection<string> GetTableSchemas()
        {
            var tableSchemas = new List<string>();
            foreach (var tableProperty in _tables)
            {
                var tableExtendedType = tableProperty.Type;
                var columnProps = tableExtendedType.ElementType.GetProperties(PropertyOptions.HasGetter);
                var tableSchema = new List<string>();
                var tablePostSchemaText = new List<string>();
                foreach (var columnProp in columnProps)
                {
                    tableSchema.Add(GetColumnSchema(tableProperty.Name, columnProp, false, out var postSchemaText, out var preTableText));
                    tablePostSchemaText.AddRange(postSchemaText);
                    if (preTableText.Any())
                        tableSchemas.Add(string.Join("\r\n", preTableText));
                }
                tableSchemas.Add(CreateTableIfNotExists(tableProperty.Name, string.Join(",\r\n", tableSchema), tablePostSchemaText));
                // also add schema new columns added
                foreach (var columnProp in columnProps)
                {
                    tableSchemas.Add(CreateTableColumnIfNotExists(tableProperty.Name, columnProp));
                }
            }
            return tableSchemas;
        }

        private string GetColumnSchema(string tableName, ExtendedProperty prop, bool includeDefaultValue, out List<string> postSchemaText, out List<string> preTableText)
        {
            postSchemaText = new List<string>();
            preTableText = new List<string>();
            var columnSchema = "";
            var defaultValue = "";
            var propExtendedType = prop.Type;
            if (propExtendedType.IsCollection)
            {
                // store as string, data will be comma delimited
                columnSchema = @$"{Quote(prop.Name)} text";
            }
            else
            {
                columnSchema = @$"{Quote(prop.Name)} ";
                switch (propExtendedType)
                {
                    case var p when p.NullableBaseType == typeof(byte):
                        columnSchema += "char";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(short):
                        columnSchema += "smallint";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(int):
                        columnSchema += "integer";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(long):
                        columnSchema += "bigint";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(double):
                        columnSchema += "float8";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(decimal):
                        columnSchema += "decimal(18, 3)";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(string):
                        columnSchema += $"text";
                        defaultValue = "''";
                        break;
                    case var p when p.NullableBaseType == typeof(DateTime):
                        columnSchema += "timestamp";
                        defaultValue = "NOW()::timestamp";
                        break;
                    case var p when p.NullableBaseType == typeof(TimeSpan):
                        columnSchema += "interval";
                        defaultValue = "NOW()::interval";
                        break;
                    case var p when p.NullableBaseType == typeof(Guid):
                        columnSchema += "uuid";
                        defaultValue = "uuid_in(overlay(overlay(md5(random()::text || ':' || random()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring)";
                        break;
                    case var p when p.NullableBaseType == typeof(bool):
                        columnSchema += "boolean";
                        defaultValue = "false";
                        break;
                    case var p when p.NullableBaseType == typeof(byte[]):
                        columnSchema += "bytea";
                        defaultValue = "''::bytea";
                        break;
                    case var p when p.NullableBaseType.IsEnum:
                        columnSchema += "integer";
                        defaultValue = "0";
                        break;
                    default:
                        throw new StorageProviderException($"Unsupported data type: {prop.Type}");
                }
            }
            if (prop.CustomAttributes.ToList().Any(x => x.AttributeType == typeof(KeyAttribute)))
            {
                if (propExtendedType.NullableBaseType != typeof(string) && propExtendedType.NullableBaseType.IsValueType)
                {
                    // add auto-increment key
                    var sequenceName = Quote(tableName + "_" + prop.Name + "_seq");
                    columnSchema += $" DEFAULT nextval('dbo.{sequenceName}'::regclass)";
                    columnSchema += " NOT NULL";
                    preTableText.Add(@$"CREATE SEQUENCE IF NOT EXISTS dbo.{sequenceName}
start 1
increment 1;
");
                }
                else
                {
                    columnSchema += " NOT NULL";
                    if (includeDefaultValue)
                        columnSchema += " DEFAULT " + defaultValue;
                }
                postSchemaText.Add(@$", CONSTRAINT ""{tableName}_pkey"" PRIMARY KEY ({Quote(prop.Name)})");
            }
            else if (propExtendedType.Type != typeof(string) && !propExtendedType.IsNullable &&
                     !propExtendedType.IsCollection)
            {
                columnSchema += " NOT NULL";
                if (includeDefaultValue)
                    columnSchema += " DEFAULT " + defaultValue;
            }

            return columnSchema;
        }

        internal static string Quote(string txt) => @$"""{txt}""";

        private string CreateTableColumnIfNotExists(string tableName, ExtendedProperty columnProp)
        {
            var columnSchema = GetColumnSchema(tableName, columnProp, true, out var _, out var _);
            return $@"ALTER TABLE dbo.{Quote(tableName)} ADD IF NOT EXISTS {columnSchema};"; // valid as of Postgresql 9.6
        }

        private string CreateTableIfNotExists(string tableName, string tableSchema, List<string> postSchemaText)
        {
            var createTable = $@"CREATE TABLE IF NOT EXISTS dbo.{Quote(tableName)} (
    {tableSchema}
";
            if (postSchemaText.Any())
                createTable += $"{string.Join("\r\n", postSchemaText)}";
            createTable += "\r\n);\r\n";
            return createTable;
        }
    }
}
