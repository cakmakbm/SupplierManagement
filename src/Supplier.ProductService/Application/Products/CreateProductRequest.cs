using System.ComponentModel.DataAnnotations;

namespace Supplier.ProductService.Application.Products;

public sealed record CreateProductRequest(
    [property: Required] Guid SupplierId,
    [property: Required, StringLength(200)] string Name,
    [property: Required, StringLength(64)] string SKU,
    [property: Range(0, double.MaxValue)] decimal Price,
    [property: Range(0, int.MaxValue)] int Stock);
