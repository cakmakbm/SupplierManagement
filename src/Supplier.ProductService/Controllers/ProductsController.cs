using Microsoft.AspNetCore.Mvc;
using Supplier.ProductService.Application.Products;

namespace Supplier.ProductService.Controllers;

[ApiController]
[Route("api/products")]
public sealed class ProductsController(CreateProductHandler handler) : ControllerBase
{
    [HttpPost]
    [ProducesResponseType<ProductResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Create(CreateProductRequest request, CancellationToken cancellationToken)
    {
        try
        {
            var product = await handler.HandleAsync(request, cancellationToken);
            return StatusCode(StatusCodes.Status201Created, product);
        }
        catch (DuplicateProductException ex)
        {
            return Problem(statusCode: StatusCodes.Status409Conflict, title: "Duplicate SKU", detail: ex.Message);
        }
    }
}
