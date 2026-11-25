using System.ComponentModel.DataAnnotations;

namespace Supplier.ProductService.Application.Products;

/// <summary>
/// [Required] does not reject Guid.Empty, which is what a missing Guid field binds to.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter)]
public sealed class NotEmptyGuidAttribute() : ValidationAttribute("The {0} field must be a non-empty GUID.")
{
    public override bool IsValid(object? value) => value is Guid guid && guid != Guid.Empty;
}
