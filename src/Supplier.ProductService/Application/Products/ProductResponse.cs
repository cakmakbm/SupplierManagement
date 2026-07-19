namespace Supplier.ProductService.Application.Products;

public sealed record ProductResponse(
    Guid Id, Guid SupplierId, string Name, string SKU,
    decimal Price, int Stock, DateTimeOffset CreatedAt);
