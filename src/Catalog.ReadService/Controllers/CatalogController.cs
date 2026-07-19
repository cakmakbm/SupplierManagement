using Catalog.ReadService.Application.Catalog;
using Catalog.ReadService.Infrastructure.Persistence;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Catalog.ReadService.Controllers;

[ApiController]
[Route("api/catalog/products")]
public sealed class CatalogController(CatalogDbContext dbContext) : ControllerBase
{
    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<CatalogProductResponse>>> Get(CancellationToken cancellationToken)
    {
        var products = await dbContext.Products.AsNoTracking().OrderByDescending(x => x.CreatedAt)
            .Select(x => new CatalogProductResponse(x.ProductId, x.SupplierId, x.Name, x.SKU,
                x.Price, x.Stock, x.CreatedAt)).ToListAsync(cancellationToken);
        return Ok(products);
    }
}
