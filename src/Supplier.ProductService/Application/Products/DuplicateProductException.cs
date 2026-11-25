namespace Supplier.ProductService.Application.Products;

public sealed class DuplicateProductException(Guid supplierId, string sku)
    : Exception($"Supplier {supplierId} already has a product with SKU '{sku}'.")
{
    public Guid SupplierId { get; } = supplierId;
    public string Sku { get; } = sku;
}
