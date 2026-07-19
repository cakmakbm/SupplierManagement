namespace Catalog.ReadService.Domain.Entities;

public sealed class CatalogProduct
{
    private CatalogProduct() { }

    public CatalogProduct(Guid productId, Guid supplierId, string name, string sku, decimal price, int stock, DateTimeOffset createdAt)
    {
        ProductId = productId;
        SupplierId = supplierId;
        Name = name;
        SKU = sku;
        Price = price;
        Stock = stock;
        CreatedAt = createdAt;
    }

    public Guid ProductId { get; private set; }
    public Guid SupplierId { get; private set; }
    public string Name { get; private set; } = string.Empty;
    public string SKU { get; private set; } = string.Empty;
    public decimal Price { get; private set; }
    public int Stock { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
}
