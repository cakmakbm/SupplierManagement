using Microsoft.AspNetCore.Mvc;
using Supplier.ProductService.Application.Products;

namespace Supplier.ProductService.Controllers;

[ApiController]
[Route("api/products")]
public sealed class ProductsController(CreateProductHandler handler) : ControllerBase
{
    [HttpPost]
    [ProducesResponseType<ProductResponse>(StatusCodes.Status201Created)]
    public async Task<IActionResult> Create(CreateProductRequest request, CancellationToken cancellationToken)
    {
        var product = await handler.HandleAsync(request, cancellationToken);
        return StatusCode(StatusCodes.Status201Created, product);
    }
}
