using System.Data.Common;
using Npgsql;

namespace Aurelon.Infrastructure.Db;

public interface IAppDbConnectionFactory
{
    Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
}

public sealed class AppDbConnectionFactory(NpgsqlDataSource dataSource) : IAppDbConnectionFactory
{
    public async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        return await dataSource.OpenConnectionAsync(cancellationToken);
    }
}
