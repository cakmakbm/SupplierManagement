using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Supplier.ProductService.Infrastructure.Persistence;

namespace Supplier.ProductService.Tests;

/// <summary>An in-memory SQLite database that lives as long as this object.</summary>
public sealed class TestDatabase : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");

    public TestDatabase()
    {
        _connection.Open();
        using var db = CreateContext();
        db.Database.EnsureCreated();
    }

    public FailingSaveInterceptor Interceptor { get; } = new();

    public ProductDbContext CreateContext() =>
        new(new DbContextOptionsBuilder<ProductDbContext>()
            .UseSqlite(_connection)
            .AddInterceptors(Interceptor)
            .Options);

    public void Dispose() => _connection.Dispose();
}
