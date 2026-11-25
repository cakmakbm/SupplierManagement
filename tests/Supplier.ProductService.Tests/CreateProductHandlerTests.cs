using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Shared.Contracts;
using Supplier.ProductService.Application.Products;

namespace Supplier.ProductService.Tests;

public sealed class CreateProductHandlerTests : IDisposable
{
    private static readonly Guid SupplierA = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid SupplierB = Guid.Parse("22222222-2222-2222-2222-222222222222");

    private readonly TestDatabase _database = new();

    public void Dispose() => _database.Dispose();

    private static CreateProductRequest Request(Guid supplierId, string sku = "DRILL-001") => new()
    {
        SupplierId = supplierId, Name = "Industrial Drill", SKU = sku, Price = 2499.90m, Stock = 25
    };

    [Fact]
    public async Task Create_SavesProductAndOutboxMessageTogether()
    {
        await using var db = _database.CreateContext();

        var response = await new CreateProductHandler(db).HandleAsync(Request(SupplierA), CancellationToken.None);

        await using var verify = _database.CreateContext();
        var product = await verify.Products.SingleAsync();
        var outbox = await verify.OutboxMessages.SingleAsync();
        var payload = JsonSerializer.Deserialize<ProductCreated>(outbox.Payload)!;

        Assert.Equal(response.Id, product.Id);
        Assert.Equal(nameof(ProductCreated), outbox.Type);
        Assert.Equal(SupplierA.ToString(), outbox.Key);
        Assert.Null(outbox.ProcessedAt);
        Assert.Equal(outbox.EventId, payload.EventId);
        Assert.Equal(product.Id, payload.ProductId);
    }

    [Fact]
    public async Task Create_WhenSaveFails_WritesNeitherProductNorOutboxMessage()
    {
        await using var db = _database.CreateContext();
        _database.Interceptor.FailNext = true;

        await Assert.ThrowsAsync<DbUpdateException>(() =>
            new CreateProductHandler(db).HandleAsync(Request(SupplierA), CancellationToken.None));

        await using var verify = _database.CreateContext();
        Assert.Equal(0, await verify.Products.CountAsync());
        Assert.Equal(0, await verify.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Create_DuplicateSkuForSameSupplier_ThrowsAndWritesNoSecondEvent()
    {
        await using (var db = _database.CreateContext())
            await new CreateProductHandler(db).HandleAsync(Request(SupplierA), CancellationToken.None);

        await using (var db = _database.CreateContext())
            await Assert.ThrowsAsync<DuplicateProductException>(() =>
                new CreateProductHandler(db).HandleAsync(Request(SupplierA, " DRILL-001 "), CancellationToken.None));

        await using var verify = _database.CreateContext();
        Assert.Equal(1, await verify.Products.CountAsync());
        Assert.Equal(1, await verify.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Create_SameSkuForDifferentSupplier_Succeeds()
    {
        await using (var db = _database.CreateContext())
            await new CreateProductHandler(db).HandleAsync(Request(SupplierA), CancellationToken.None);
        await using (var db = _database.CreateContext())
            await new CreateProductHandler(db).HandleAsync(Request(SupplierB), CancellationToken.None);

        await using var verify = _database.CreateContext();
        Assert.Equal(2, await verify.Products.CountAsync());
    }

    [Fact]
    public void Validation_RejectsEmptySupplierId()
    {
        var request = Request(Guid.Empty);
        var results = new List<ValidationResult>();

        var valid = Validator.TryValidateObject(request, new ValidationContext(request), results, validateAllProperties: true);

        Assert.False(valid);
        Assert.Contains(results, r => r.MemberNames.Contains(nameof(CreateProductRequest.SupplierId)));
    }
}
