namespace Supplier.ProductService.Domain.Entities;

public sealed class Product
{
    private Product() { }

    public Product(Guid supplierId, string name, string sku, decimal price, int stock)
    {
        Id = Guid.NewGuid();
        SupplierId = supplierId;
        Name = name;
        SKU = sku;
        Price = price;
        Stock = stock;
        CreatedAt = DateTimeOffset.UtcNow;
    }

    public Guid Id { get; private set; }
    public Guid SupplierId { get; private set; }
    public string Name { get; private set; } = string.Empty;
    public string SKU { get; private set; } = string.Empty;
    public decimal Price { get; private set; }
    public int Stock { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
}
