using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Supplier.ProductService.Infrastructure.Persistence;

namespace Supplier.ProductService.Tests;

/// <summary>
/// Runs requests through the real ASP.NET Core pipeline (routing, model binding, validation),
/// with SQLite instead of SQL Server and without the Kafka background worker.
/// </summary>
public sealed class ProductsApiTests : IClassFixture<ProductsApiTests.ApiFactory>
{
    private readonly HttpClient _client;

    public ProductsApiTests(ApiFactory factory) => _client = factory.CreateClient();

    private static object Body(string supplierId, string sku) =>
        new { supplierId, name = "Industrial Drill", sku, price = 2499.90m, stock = 25 };

    [Fact]
    public async Task Post_ValidProduct_Returns201()
    {
        var response = await _client.PostAsJsonAsync("/api/products",
            Body("11111111-1111-1111-1111-111111111111", "API-1"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    [Fact]
    public async Task Post_DuplicateSku_Returns409()
    {
        var body = Body("22222222-2222-2222-2222-222222222222", "API-2");
        await _client.PostAsJsonAsync("/api/products", body);

        var response = await _client.PostAsJsonAsync("/api/products", body);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
    }

    [Fact]
    public async Task Post_EmptySupplierId_Returns400()
    {
        var response = await _client.PostAsJsonAsync("/api/products",
            Body("00000000-0000-0000-0000-000000000000", "API-3"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("SupplierId", await response.Content.ReadAsStringAsync());
    }

    public sealed class ApiFactory : WebApplicationFactory<Program>
    {
        private readonly SqliteConnection _connection = new("DataSource=:memory:");

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            _connection.Open();
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<DbContextOptions<ProductDbContext>>();
                services.AddDbContext<ProductDbContext>(o => o.UseSqlite(_connection));
                // No SQL Server initializer and no Kafka worker in these tests.
                foreach (var hosted in services.Where(d => d.ServiceType == typeof(IHostedService)).ToList())
                    services.Remove(hosted);
            });
        }

        protected override IHost CreateHost(IHostBuilder builder)
        {
            var host = base.CreateHost(builder);
            using var scope = host.Services.CreateScope();
            scope.ServiceProvider.GetRequiredService<ProductDbContext>().Database.EnsureCreated();
            return host;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing) _connection.Dispose();
        }
    }
}
