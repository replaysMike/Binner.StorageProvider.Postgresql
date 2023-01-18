using Binner.Model.Common;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using TypeSupport;
using TypeSupport.Extensions;
using static Binner.Model.Common.SystemDefaults;

namespace Binner.StorageProvider.Postgresql
{
    /// <summary>
    /// Binner storage provider for Postgresql
    /// </summary>
    public class PostgresqlStorageProvider : IStorageProvider
    {
        public const string ProviderName = "Postgresql";

        private readonly PostgresqlStorageConfiguration _config;
        private bool _isDisposed;

        public PostgresqlStorageProvider(IDictionary<string, string> config)
        {
            _config = new PostgresqlStorageConfiguration(config);
            try
            {
                GenerateDatabaseIfNotExistsAsync<IBinnerDb>()
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception ex)
            {
                throw new StorageProviderException(nameof(PostgresqlStorageProvider), $"Failed to generate database! {ex.GetType().Name} = {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Get an instance of the entire database
        /// </summary>
        /// <returns></returns>
        public async Task<IBinnerDb> GetDatabaseAsync(IUserContext userContext)
        {
            var parts = await GetPartsAsync();
            return new BinnerDbV1
            {
                OAuthCredentials = await GetOAuthCredentialAsync(userContext),
                Parts = parts,
                PartTypes = await GetPartTypesAsync(userContext),
                Projects = await GetProjectsAsync(userContext),
                Count = parts.Count,
                FirstPartId = parts.OrderBy(x => x.PartId).First().PartId,
                LastPartId = parts.OrderBy(x => x.PartId).Last().PartId,
            };
        }

        public async Task<long> GetUniquePartsCountAsync(IUserContext userContext)
        {
            var query = @$"SELECT COUNT(*) FROM dbo.""Parts"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await ExecuteScalarAsync<long>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<long> GetPartsCountAsync(IUserContext userContext)
        {
            var query = @$"SELECT CAST(SUM(""Quantity"") AS bigint) FROM dbo.""Parts"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await ExecuteScalarAsync<long>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<decimal> GetPartsValueAsync(IUserContext userContext)
        {
            var query = @$"SELECT SUM(""Cost"" * ""Quantity"") FROM dbo.""Parts"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await ExecuteScalarAsync<decimal>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<PaginatedResponse<Part>> GetLowStockAsync(PaginatedRequest request, IUserContext userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";

            var parameters = new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = (int)request.Direction,
                UserId = userContext?.UserId
            };
            var countQuery = $"SELECT COUNT(*) FROM dbo.\"Parts\" WHERE \"Quantity\" <= \"LowStockThreshold\" AND (@UserId::integer IS NULL OR \"UserId\" = @UserId);";
            var totalPages = await ExecuteScalarAsync<int>(countQuery, parameters);

            var query =
$@"SELECT * FROM dbo.""Parts""
WHERE ""Quantity"" <= ""LowStockThreshold"" AND (@UserId::integer IS NULL OR ""UserId"" = @UserId)
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN ""PartId"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartNumber' THEN ""PartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DigikeyPartNumber' THEN ""DigiKeyPartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'MouserPartNumber' THEN ""MouserPartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Cost' THEN ""Cost"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Quantity' THEN ""Quantity"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'LowStockThreshold' THEN ""LowStockThreshold"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartTypeId' THEN ""PartTypeId"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ProjectId' THEN ""ProjectId"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN ""Location"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber' THEN ""BinNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber2' THEN ""BinNumber2"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Manufacturer' THEN ""Manufacturer"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ManufacturerPartNumber' THEN ""ManufacturerPartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN ""DateCreatedUtc"" ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Part>(query, parameters);
            return new PaginatedResponse<Part>(totalPages, result);
        }

        public async Task<Part> AddPartAsync(Part part, IUserContext userContext)
        {
            part.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO dbo.""Parts"" (""Quantity"", ""LowStockThreshold"", ""PartNumber"", ""PackageType"", ""MountingTypeId"", ""DigiKeyPartNumber"", ""MouserPartNumber"", ""Description"", ""PartTypeId"", ""ProjectId"", ""Keywords"", ""DatasheetUrl"", ""Location"", ""BinNumber"", ""BinNumber2"", ""UserId"", ""Cost"", ""Manufacturer"", ""ManufacturerPartNumber"", ""LowestCostSupplier"", ""LowestCostSupplierUrl"", ""ProductUrl"", ""ImageUrl"", ""DateCreatedUtc"") 
VALUES(@Quantity, @LowStockThreshold, @PartNumber, @PackageType, @MountingTypeId, @DigiKeyPartNumber, @MouserPartNumber, @Description, @PartTypeId, @ProjectId, @Keywords, @DatasheetUrl, @Location, @BinNumber, @BinNumber2, @UserId, @Cost, @Manufacturer, @ManufacturerPartNumber, @LowestCostSupplier, @LowestCostSupplierUrl, @ProductUrl, @ImageUrl, @DateCreatedUtc)
RETURNING ""PartId"";
";
            return await InsertAsync<Part, long>(query, part, (x, key) => { x.PartId = key; });
        }

        public async Task<Project> AddProjectAsync(Project project, IUserContext userContext)
        {
            project.UserId = userContext?.UserId;
            var query =
$@"INSERT INTO dbo.""Projects"" (""Name"", ""Description"", ""Location"", ""Color"", ""UserId"", ""DateCreatedUtc"") 
VALUES(@Name, @Description, @Location, @Color, @UserId, @DateCreatedUtc)
RETURNING ""ProjectId"";
";
            return await InsertAsync<Project, long>(query, project, (x, key) => { x.ProjectId = key; });
        }

        public async Task<bool> DeletePartAsync(Part part, IUserContext userContext)
        {
            part.UserId = userContext?.UserId;
            var query = @$"DELETE FROM dbo.""Parts"" WHERE ""PartId"" = @PartId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            return await ExecuteAsync(query, part) > 0;
        }

        public async Task<bool> DeletePartTypeAsync(PartType partType, IUserContext userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = @$"DELETE FROM dbo.""PartTypes"" WHERE ""PartTypeId"" = @PartTypeId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            return await ExecuteAsync(query, partType) > 0;
        }

        public async Task<bool> DeleteProjectAsync(Project project, IUserContext userContext)
        {
            project.UserId = userContext?.UserId;
            var query = @$"DELETE FROM dbo.""Projects"" WHERE ""ProjectId"" = @ProjectId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            return await ExecuteAsync(query, project) > 0;
        }

        public async Task<ICollection<SearchResult<Part>>> FindPartsAsync(string keywords, IUserContext userContext)
        {
            // basic ranked search by Michael Brown :)
            var query =
$@"WITH ""PartsExactMatch"" (""PartId"", ""Rank"") AS
(
SELECT ""PartId"", 10 as ""Rank"" FROM dbo.""Parts"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId) AND 
""PartNumber"" ILIKE @Keywords 
OR ""DigiKeyPartNumber"" ILIKE @Keywords 
OR ""MouserPartNumber"" ILIKE @Keywords
OR ""ManufacturerPartNumber"" ILIKE @Keywords
OR ""Description"" ILIKE @Keywords 
OR ""Keywords"" ILIKE @Keywords 
OR ""Location"" ILIKE @Keywords 
OR ""BinNumber"" ILIKE @Keywords 
OR ""BinNumber2"" ILIKE @Keywords
),
""PartsBeginsWith"" (""PartId"", ""Rank"") AS
(
SELECT ""PartId"", 100 as ""Rank"" FROM dbo.""Parts"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId) AND 
""PartNumber"" ILIKE CONCAT(@Keywords, '%')
OR ""DigiKeyPartNumber"" ILIKE CONCAT(@Keywords, '%')
OR ""MouserPartNumber"" ILIKE CONCAT(@Keywords, '%')
OR ""ManufacturerPartNumber"" ILIKE CONCAT(@Keywords, '%')
OR ""Description"" ILIKE CONCAT(@Keywords, '%')
OR ""Keywords"" ILIKE CONCAT(@Keywords, '%')
OR ""Location"" ILIKE CONCAT(@Keywords, '%')
OR ""BinNumber"" ILIKE CONCAT(@Keywords, '%')
OR ""BinNumber2"" ILIKE CONCAT(@Keywords, '%')
),
""PartsAny"" (""PartId"", ""Rank"") AS
(
SELECT ""PartId"", 200 as ""Rank"" FROM dbo.""Parts"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId) AND 
""PartNumber"" ILIKE CONCAT('%', @Keywords, '%')
OR ""DigiKeyPartNumber"" ILIKE CONCAT('%', @Keywords, '%')
OR ""MouserPartNumber"" ILIKE CONCAT('%', @Keywords, '%')
OR ""ManufacturerPartNumber"" ILIKE CONCAT('%', @Keywords, '%')
OR ""Description"" ILIKE CONCAT('%', @Keywords, '%')
OR ""Keywords"" ILIKE CONCAT('%', @Keywords, '%')
OR ""Location"" ILIKE CONCAT('%', @Keywords, '%')
OR ""BinNumber"" ILIKE CONCAT('%', @Keywords, '%')
OR ""BinNumber2"" ILIKE CONCAT('%', @Keywords, '%')
),
""PartsMerged"" (""PartId"", ""Rank"") AS
(
	SELECT ""PartId"", ""Rank"" FROM ""PartsExactMatch""
	UNION
	SELECT ""PartId"", ""Rank"" FROM ""PartsBeginsWith""
	UNION
	SELECT ""PartId"", ""Rank"" FROM ""PartsAny""
)
SELECT pm.""Rank"", p.* FROM dbo.""Parts"" p
INNER JOIN (
  SELECT ""PartId"", MIN(""Rank"") ""Rank"" FROM ""PartsMerged"" GROUP BY ""PartId""
) pm ON pm.""PartId"" = p.""PartId"" ORDER BY pm.""Rank"" ASC;
;";
            var result = await SqlQueryAsync<PartSearch>(query, new { Keywords = keywords, UserId = userContext?.UserId });
            return result.Select(x => new SearchResult<Part>(x as Part, x.Rank)).OrderBy(x => x.Rank).ToList();
        }

        private async Task<ICollection<OAuthCredential>> GetOAuthCredentialAsync(IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""OAuthCredentials"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<OAuthCredential> GetOAuthCredentialAsync(string providerName, IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""OAuthCredentials"" WHERE ""Provider"" = @ProviderName AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, new { ProviderName = providerName, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<PartType> GetOrCreatePartTypeAsync(PartType partType, IUserContext userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = @$"SELECT ""PartTypeId"" FROM dbo.""PartTypes"" WHERE ""Name"" = @Name AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, partType);
            if (result.Any())
            {
                return result.FirstOrDefault();
            }
            else
            {
                query =
$@"INSERT INTO dbo.""PartTypes"" (""ParentPartTypeId"", ""Name"", ""UserId"", ""DateCreatedUtc"") 
VALUES (@ParentPartTypeId, @Name, @UserId, @DateCreatedUtc)
RETURNING ""PartTypeId"";";
                partType = await InsertAsync<PartType, long>(query, partType, (x, key) => { x.PartTypeId = key; });
            }
            return partType;
        }

        public async Task<ICollection<PartType>> GetPartTypesAsync(IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""PartTypes"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId) OR ""UserId"" IS NULL;";
            var result = await SqlQueryAsync<PartType>(query, new { UserId = userContext?.UserId });
            return result.ToList();
        }

        public async Task<Part> GetPartAsync(long partId, IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""Parts"" WHERE ""PartId"" = @PartId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<Part>(query, new { PartId = partId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Part> GetPartAsync(string partNumber, IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""Parts"" WHERE ""PartNumber"" = @PartNumber AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<Part>(query, new { PartNumber = partNumber, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        private async Task<ICollection<Part>> GetPartsAsync()
        {
            var query = @$"SELECT * FROM dbo.""Parts"";";
            var result = await SqlQueryAsync<Part>(query);
            return result;
        }

        public async Task<ICollection<Part>> GetPartsAsync(Expression<Func<Part, bool>> predicate, IUserContext userContext)
        {
            var conditionalQuery = TranslatePredicateToSql(predicate);
            var query = @$"SELECT * FROM dbo.""Parts"" WHERE {conditionalQuery.Sql} AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            conditionalQuery.Parameters.Add("UserId", userContext?.UserId);
            var result = await SqlQueryAsync<Part>(query, conditionalQuery.Parameters);
            return result.ToList();
        }

        public async Task<PaginatedResponse<Part>> GetPartsAsync(PaginatedRequest request, IUserContext userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var binFilter = "";

            if (request.By != null)
            {
                binFilter = $" AND {request.By} = '{request.Value}'";
            }

            var parameters = new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = (int)request.Direction,
                UserId = userContext?.UserId
            };

            var countQuery = $"SELECT COUNT(*) FROM dbo.\"Parts\" WHERE (@UserId::integer IS NULL OR \"UserId\" = @UserId) {binFilter};";
            var totalPages = await ExecuteScalarAsync<int>(countQuery, parameters);

            var query =
$@"SELECT * FROM dbo.""Parts""
WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId) {binFilter}
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN ""PartId"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartNumber' THEN ""PartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DigikeyPartNumber' THEN ""DigiKeyPartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'MouserPartNumber' THEN ""MouserPartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Cost' THEN ""Cost"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Quantity' THEN ""Quantity"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'LowStockThreshold' THEN ""LowStockThreshold"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'PartTypeId' THEN ""PartTypeId"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ProjectId' THEN ""ProjectId"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN ""Location"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber' THEN ""BinNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'BinNumber2' THEN ""BinNumber2"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Manufacturer' THEN ""Manufacturer"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'ManufacturerPartNumber' THEN ""ManufacturerPartNumber"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN ""DateCreatedUtc"" ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Part>(query, parameters);
            return new PaginatedResponse<Part>(totalPages, result.ToList());
        }

        public async Task<PartType> GetPartTypeAsync(long partTypeId, IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""PartTypes"" WHERE ""PartTypeId"" = @PartTypeId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, new { PartTypeId = partTypeId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Project> GetProjectAsync(long projectId, IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""Projects"" WHERE ""ProjectId"" = @ProjectId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { ProjectId = projectId, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        public async Task<Project> GetProjectAsync(string projectName, IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""Projects"" WHERE ""Name"" = @Name AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { Name = projectName, UserId = userContext?.UserId });
            return result.FirstOrDefault();
        }

        private async Task<ICollection<Project>> GetProjectsAsync(IUserContext userContext)
        {
            var query = @$"SELECT * FROM dbo.""Projects"" WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<Project>(query, new { UserId = userContext?.UserId });
            return result;
        }

        public async Task<ICollection<Project>> GetProjectsAsync(PaginatedRequest request, IUserContext userContext)
        {
            var offsetRecords = (request.Page - 1) * request.Results;
            var sortDirection = request.Direction == SortDirection.Ascending ? "ASC" : "DESC";
            var query =
$@"SELECT * FROM dbo.""Projects"" 
WHERE (@UserId::integer IS NULL OR ""UserId"" = @UserId) 
ORDER BY 
CASE WHEN @OrderBy IS NULL THEN ""ProjectId"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Name' THEN ""Name"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Description' THEN ""Description"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'Location' THEN ""Location"" ELSE NULL END {sortDirection}, 
CASE WHEN @OrderBy = 'DateCreatedUtc' THEN ""DateCreatedUtc"" ELSE NULL END {sortDirection} 
OFFSET {offsetRecords} ROWS FETCH NEXT {request.Results} ROWS ONLY;";
            var result = await SqlQueryAsync<Project>(query, new
            {
                Results = request.Results,
                Page = request.Page,
                OrderBy = request.OrderBy,
                Direction = (int)request.Direction,
                UserId = userContext?.UserId
            });
            return result.ToList();
        }

        public async Task RemoveOAuthCredentialAsync(string providerName, IUserContext userContext)
        {
            var query = @$"DELETE FROM dbo.""OAuthCredentials"" WHERE ""Provider"" = @Provider AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            await ExecuteAsync<object>(query, new { Provider = providerName, UserId = userContext?.UserId });
        }

        public async Task<OAuthCredential> SaveOAuthCredentialAsync(OAuthCredential credential, IUserContext userContext)
        {
            credential.UserId = userContext?.UserId;
            var query = @"SELECT ""Provider"" FROM dbo.""OAuthCredentials"" WHERE ""Provider"" = @Provider AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<OAuthCredential>(query, credential);
            if (result.Any())
            {
                query = $@"UPDATE dbo.""OAuthCredentials"" SET ""AccessToken"" = @AccessToken, ""RefreshToken"" = @RefreshToken, ""DateCreatedUtc"" = @DateCreatedUtc, ""DateExpiresUtc"" = @DateExpiresUtc WHERE ""Provider"" = @Provider AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
                await ExecuteAsync<object>(query, credential);
            }
            else
            {
                query =
$@"INSERT INTO dbo.""OAuthCredentials"" (""Provider"", ""AccessToken"", ""RefreshToken"", ""DateCreatedUtc"", ""DateExpiresUtc"", ""UserId"") 
VALUES (@Provider, @AccessToken, @RefreshToken, @DateCreatedUtc, @DateExpiresUtc, @UserId);";
                await InsertAsync<OAuthCredential, string>(query, credential, (x, key) => { });
            }
            return credential;
        }

        public async Task<Part> UpdatePartAsync(Part part, IUserContext userContext)
        {
            part.UserId = userContext?.UserId;
            var query = @$"SELECT ""PartId"" FROM dbo.""Parts"" WHERE ""PartId"" = @PartId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<Part>(query, part);
            if (result.Any())
            {
                query = @$"UPDATE dbo.""Parts"" SET ""Quantity"" = @Quantity, ""LowStockThreshold"" = @LowStockThreshold, ""Cost"" = @Cost, ""PartNumber"" = @PartNumber, ""PackageType"" = @PackageType, ""MountingTypeId"" = @MountingTypeId, ""DigiKeyPartNumber"" = @DigiKeyPartNumber, ""MouserPartNumber"" = @MouserPartNumber, ""Description"" = @Description, ""PartTypeId"" = @PartTypeId, ""ProjectId"" = @ProjectId, ""Keywords"" = @Keywords, ""DatasheetUrl"" = @DatasheetUrl, ""Location"" = @Location, ""BinNumber"" = @BinNumber, ""BinNumber2"" = @BinNumber2, ""ProductUrl"" = @ProductUrl, ""ImageUrl"" = @ImageUrl, ""LowestCostSupplier"" = @LowestCostSupplier, ""LowestCostSupplierUrl"" = @LowestCostSupplierUrl, ""Manufacturer"" = @Manufacturer, ""ManufacturerPartNumber"" = @ManufacturerPartNumber WHERE ""PartId"" = @PartId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
                await ExecuteAsync(query, part);
            }
            else
            {
                throw new StorageProviderException(nameof(PostgresqlStorageProvider), $"Record not found for {nameof(Part)} = {part.PartId}");
            }
            return part;
        }

        public async Task<PartType> UpdatePartTypeAsync(PartType partType, IUserContext userContext)
        {
            partType.UserId = userContext?.UserId;
            var query = @$"SELECT ""PartTypeId"" FROM dbo.""PartTypes"" WHERE ""PartTypeId"" = @PartTypeId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<PartType>(query, partType);
            if (result.Any())
            {
                query = @$"UPDATE dbo.""PartTypes"" SET ""Name"" = @Name, ""ParentPartTypeId"" = @ParentPartTypeId WHERE ""PartTypeId"" = @PartTypeId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
                await ExecuteAsync(query, partType);
            }
            else
            {
                throw new StorageProviderException(nameof(PostgresqlStorageProvider), $"Record not found for {nameof(PartType)} = {partType.PartTypeId}");
            }
            return partType;
        }

        public async Task<Project> UpdateProjectAsync(Project project, IUserContext userContext)
        {
            project.UserId = userContext?.UserId;
            var query = @$"SELECT ""ProjectId"" FROM dbo.""Projects"" WHERE ""ProjectId"" = @ProjectId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
            var result = await SqlQueryAsync<Project>(query, project);
            if (result.Any())
            {
                query = @$"UPDATE dbo.""Projects"" SET ""Name"" = @Name, ""Description"" = @Description, ""Location"" = @Location, ""Color"" = @Color WHERE ""ProjectId"" = @ProjectId AND (@UserId::integer IS NULL OR ""UserId"" = @UserId);";
                await ExecuteAsync<Project>(query, project);
            }
            else
            {
                throw new StorageProviderException(nameof(PostgresqlStorageProvider), $"Record not found for {nameof(Project)} = {project.ProjectId}");
            }
            return project;
        }

        private async Task<T> InsertAsync<T, TKey>(string query, T parameters, Action<T, TKey> keySetter)
        {
            using (var connection = new NpgsqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new NpgsqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters<T>(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var result = await sqlCmd.ExecuteScalarAsync();
                    if (result != null)
                        keySetter.Invoke(parameters, (TKey)result);
                }
                connection.Close();
            }
            return parameters;
        }

        private async Task<ICollection<T>> SqlQueryAsync<T>(string query, object parameters = null)
        {
            var results = new List<T>();
            var type = typeof(T).GetExtendedType();
            using (var connection = new NpgsqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new NpgsqlCommand(query, connection))
                {
                    if (parameters != null)
                        sqlCmd.Parameters.AddRange(CreateParameters(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var reader = await sqlCmd.ExecuteReaderAsync();
                    while (reader.Read())
                    {
                        var newObj = Activator.CreateInstance<T>();
                        foreach (var prop in type.Properties)
                        {
                            if (reader.HasColumn(prop.Name))
                            {
                                var val = MapToPropertyValue(reader[prop.Name], prop.Type);
                                newObj.SetPropertyValue(prop.PropertyInfo, val);
                            }
                        }
                        results.Add(newObj);
                    }
                }
                connection.Close();
            }
            return results;
        }

        private async Task<T> ExecuteScalarAsync<T>(string query, object parameters = null)
        {
            T result;
            using (var connection = new NpgsqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new NpgsqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters(parameters));
                    sqlCmd.CommandType = CommandType.Text;
                    var untypedResult = await sqlCmd.ExecuteScalarAsync();
                    if (untypedResult != DBNull.Value)
                        result = (T)untypedResult;
                    else
                        result = default(T);
                }
                connection.Close();
            }
            return result;
        }

        private async Task<int> ExecuteAsync<T>(string query, T record)
        {
            var modified = 0;
            using (var connection = new NpgsqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new NpgsqlCommand(query, connection))
                {
                    sqlCmd.Parameters.AddRange(CreateParameters<T>(record));
                    sqlCmd.CommandType = CommandType.Text;
                    modified = await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            return modified;
        }

        private NpgsqlParameter[] CreateParameters<T>(T record)
        {
            var parameters = new List<NpgsqlParameter>();
            var extendedType = record.GetExtendedType();
            if (extendedType.IsDictionary)
            {
                var t = record as IDictionary<string, object>;
                foreach (var p in t)
                {
                    var key = p.Key;
                    var val = p.Value;
                    var propertyMapped = MapFromPropertyValue(val);
                    parameters.Add(new NpgsqlParameter(key.ToString(), propertyMapped));
                }
            }
            else
            {
                var properties = record.GetProperties(PropertyOptions.HasGetter);
                foreach (var property in properties)
                {
                    var propertyValue = record.GetPropertyValue(property);
                    var propertyMapped = MapFromPropertyValue(propertyValue);
                    if (propertyValue is null)
                    {
                        var dbType = GetDbType(property.Type);
                        parameters.Add(new NpgsqlParameter(property.Name, dbType) { Value = propertyMapped });
                    }
                    else
                    {
                        parameters.Add(new NpgsqlParameter(property.Name, propertyMapped));
                    }
                }
            }
            return parameters.ToArray();
        }

        private DbType GetDbType(ExtendedType type)
        {
            var @switch = new Dictionary<Type, DbType> {
                { typeof(byte), DbType.Byte },
                { typeof(byte?), DbType.Byte },
                { typeof(decimal), DbType.Decimal },
                { typeof(decimal?), DbType.Decimal },
                { typeof(double), DbType.Double },
                { typeof(double?), DbType.Double },
                { typeof(int), DbType.Int32 },
                { typeof(int?), DbType.Int32 },
                { typeof(long), DbType.Int64 },
                { typeof(long?), DbType.Int64 },
                { typeof(short), DbType.Int16 },
                { typeof(short?), DbType.Int16 },
                { typeof(Guid), DbType.Guid },
                { typeof(Guid?), DbType.Guid },
                { typeof(bool), DbType.Boolean },
                { typeof(bool?), DbType.Boolean },
                { typeof(string), DbType.String },
                { typeof(DateTime), DbType.DateTime },
                { typeof(TimeSpan), DbType.Time },
                { typeof(byte[]), DbType.Binary },
            };

            if (type.Type.IsEnum)
                return type.IsNullable ? @switch[typeof(int?)] : @switch[typeof(int)];

            if (@switch.ContainsKey(type.Type))
                return @switch[type.Type];

            throw new StorageProviderException(nameof(PostgresqlStorageProvider), $"No database type mapping available for type '{type.Type}'");
        }

        private static object MapToPropertyValue(object obj, Type destinationType)
        {
            if (obj == DBNull.Value) return null;

            var objType = destinationType.GetExtendedType();
            switch (objType)
            {
                case var p when p.IsCollection:
                case var a when a.IsArray:
                    return obj.ToString().Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                default:
                    return obj;
            }
        }

        private static object MapFromPropertyValue(object obj)
        {
            if (obj == null) return DBNull.Value;

            var objType = obj.GetExtendedType();
            switch (objType)
            {
                case var p when p.IsCollection:
                case var a when a.IsArray:
                    return string.Join(",", ((ICollection<string>)obj).Select(x => x.ToString()).ToArray());
                case var p when p.Type == typeof(DateTime):
                    if (((DateTime)obj) == DateTime.MinValue)
                        return SqlDateTime.MinValue.Value;
                    return obj;
                default:
                    return obj;
            }
        }

        private async Task<bool> GenerateDatabaseIfNotExistsAsync<T>()
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(_config.ConnectionString);
            var schemaGenerator = new PostgresqlServerSchemaGenerator<T>(connectionStringBuilder.Database ?? string.Empty);
            var modified = 0;
            var partTypesCount = 0l;

            // Ensure database exists
            var query = schemaGenerator.CreateDatabaseIfNotExists();
            using (var connection = new NpgsqlConnection(GetMasterDbConnectionString(_config.ConnectionString)))
            {
                connection.Open();
                using (var sqlCmd = new NpgsqlCommand(query, connection))
                {
                    try
                    {
                        await sqlCmd.ExecuteNonQueryAsync();
                        modified++;
                    }
                    catch (PostgresException ex)
                    {
                        if (ex.Message.Contains("already exists"))
                        {
                            // database already exists, do nothing
                        }
                        else
                        {
                            throw new StorageProviderException(nameof(PostgresqlStorageProvider), "Error creating database! See inner exception for details.", ex);
                        }
                    }
                }
                connection.Close();
            }
            // Ensure table schema exists
            query = schemaGenerator.CreateTableSchemaIfNotExists();
            using (var connection = new NpgsqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new NpgsqlCommand(query, connection))
                {
                    await sqlCmd.ExecuteNonQueryAsync();
                }
                // if seed data is missing, run the initial seed
                using (var sqlCmd = new NpgsqlCommand($"SELECT COUNT(*) FROM dbo.{Quote("PartTypes")}", connection))
                {
                    partTypesCount = (long)await sqlCmd.ExecuteScalarAsync();
                }
                connection.Close();
            }
            if (partTypesCount == 0) await SeedInitialDataAsync();

            return partTypesCount == 0;
        }

        private async Task<bool> SeedInitialDataAsync()
        {
            //DefaultPartTypes
            var defaultPartTypes = typeof(SystemDefaults.DefaultPartTypes).GetExtendedType();
            var query = "";
            var modified = 0;
            foreach (var partType in defaultPartTypes.EnumValues)
            {
                int? parentPartTypeId = null;
                var partTypeEnum = (DefaultPartTypes)partType.Key;
                var field = typeof(DefaultPartTypes).GetField(partType.Value);
                if (field.IsDefined(typeof(ParentPartTypeAttribute), false))
                {
                    var customAttribute = Attribute.GetCustomAttribute(field, typeof(ParentPartTypeAttribute)) as ParentPartTypeAttribute;
                    parentPartTypeId = (int)customAttribute.Parent;
                }

                query += $"INSERT INTO dbo.{Quote("PartTypes")} ({Quote("Name")}, {Quote("ParentPartTypeId")}, {Quote("DateCreatedUtc")}) VALUES('{partType.Value}', {parentPartTypeId?.ToString() ?? "null"}, timezone('utc'::text, now()));\r\n";
            }
            using (var connection = new NpgsqlConnection(_config.ConnectionString))
            {
                connection.Open();
                using (var sqlCmd = new NpgsqlCommand(query, connection))
                {
                    modified = await sqlCmd.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            return modified > 0;
        }

        private static WhereCondition TranslatePredicateToSql(Expression<Func<Part, bool>> predicate)
        {
            var builder = new SqlWhereExpressionBuilder();
            var sql = builder.ToParameterizedSql(predicate, true, false);
            return sql;
        }

        private static string GetMasterDbConnectionString(string connectionString)
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            builder.Database = "postgres";
            return builder.ToString();
        }

        private static string Quote(string txt) => PostgresqlServerSchemaGenerator<IBinnerDb>.Quote(txt);

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool isDisposing)
        {
            if (_isDisposed)
                return;
            if (isDisposing)
            {

            }
            _isDisposed = true;
        }
    }
}