namespace Catalog.ReadService.Application.Catalog;

public sealed record CatalogProductResponse(
    Guid ProductId, Guid SupplierId, string Name, string SKU,
    decimal Price, int Stock, DateTimeOffset CreatedAt);
