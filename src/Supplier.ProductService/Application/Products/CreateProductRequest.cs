using System.ComponentModel.DataAnnotations;

namespace Supplier.ProductService.Application.Products;

// Not a positional record: MVC rejects records whose validation attributes sit on properties
// generated from the primary constructor, and every POST then failed with 500.
public sealed record CreateProductRequest
{
    [Required, NotEmptyGuid]
    public Guid SupplierId { get; init; }

    [Required, StringLength(200)]
    public string Name { get; init; } = string.Empty;

    [Required, StringLength(64)]
    public string SKU { get; init; } = string.Empty;

    [Range(0, double.MaxValue)]
    public decimal Price { get; init; }

    [Range(0, int.MaxValue)]
    public int Stock { get; init; }
}
