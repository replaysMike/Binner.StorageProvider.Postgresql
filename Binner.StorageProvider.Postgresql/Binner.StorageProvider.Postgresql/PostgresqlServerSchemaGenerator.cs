using Binner.Model.Common;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
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
                    tableSchema.Add(GetColumnSchema(tableProperty.Name, columnProp, out var postSchemaText, out var preTableText));
                    tablePostSchemaText.AddRange(postSchemaText);
                    if (preTableText.Any())
                        tableSchemas.Add(string.Join("\r\n", preTableText));
                }
                tableSchemas.Add(CreateTableIfNotExists(tableProperty.Name, string.Join(",\r\n", tableSchema), tablePostSchemaText));
            }
            return tableSchemas;
        }

        private string GetColumnSchema(string tableName, ExtendedProperty prop, out List<string> postSchemaText, out List<string> preTableText)
        {
            postSchemaText = new List<string>();
            preTableText = new List<string>();
            var columnSchema = "";
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
                        break;
                    case var p when p.NullableBaseType == typeof(short):
                        columnSchema += "smallint";
                        break;
                    case var p when p.NullableBaseType == typeof(int):
                        columnSchema += "integer";
                        break;
                    case var p when p.NullableBaseType == typeof(long):
                        columnSchema += "bigint";
                        break;
                    case var p when p.NullableBaseType == typeof(double):
                        columnSchema += "float8";
                        break;
                    case var p when p.NullableBaseType == typeof(decimal):
                        columnSchema += "decimal(18, 3)";
                        break;
                    case var p when p.NullableBaseType == typeof(string):
                        columnSchema += $"text";
                        break;
                    case var p when p.NullableBaseType == typeof(DateTime):
                        columnSchema += "timestamp";
                        break;
                    case var p when p.NullableBaseType == typeof(TimeSpan):
                        columnSchema += "interval";
                        break;
                    case var p when p.NullableBaseType == typeof(byte[]):
                        columnSchema += "bytea";
                        break;
                    case var p when p.NullableBaseType.IsEnum:
                        columnSchema += "integer";
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
                    preTableText.Add(@$"CREATE SEQUENCE IF NOT EXISTS dbo.{sequenceName}
start 1
increment 1;
");
                }
                columnSchema = columnSchema + " NOT NULL";
                postSchemaText.Add(@$", CONSTRAINT ""{tableName}_pkey"" PRIMARY KEY ({Quote(prop.Name)})");
            }
            else if (propExtendedType.Type != typeof(string) && !propExtendedType.IsNullable && !propExtendedType.IsCollection)
                columnSchema = columnSchema + " NOT NULL";
            return columnSchema;
        }

        internal static string Quote(string txt) => @$"""{txt}""";

        private string GetMaxLength(ExtendedProperty prop)
        {
            var maxLengthAttr = prop.CustomAttributes.ToList().FirstOrDefault(x => x.AttributeType == typeof(MaxLengthAttribute));
            var maxLength = "max";
            if (maxLengthAttr != null)
            {
                maxLength = maxLengthAttr.ConstructorArguments.First().Value.ToString();
            }
            return maxLength;
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
