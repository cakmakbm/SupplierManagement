using Shared.Contracts;
using Supplier.ProductService.Application.Abstractions;
using Supplier.ProductService.Domain.Entities;
using Supplier.ProductService.Infrastructure.Persistence;

namespace Supplier.ProductService.Application.Products;

public sealed class CreateProductHandler(ProductDbContext dbContext, IProductEventPublisher publisher)
{
    public async Task<ProductResponse> HandleAsync(CreateProductRequest request, CancellationToken cancellationToken)
    {
        var product = new Product(request.SupplierId, request.Name.Trim(), request.SKU.Trim(), request.Price, request.Stock);
        dbContext.Products.Add(product);
        await dbContext.SaveChangesAsync(cancellationToken);

        var message = new ProductCreated(
            Guid.NewGuid(), product.Id, product.SupplierId, product.Name, product.SKU,
            product.Price, product.Stock, product.CreatedAt);
        await publisher.PublishAsync(message, cancellationToken);

        return new ProductResponse(product.Id, product.SupplierId, product.Name, product.SKU,
            product.Price, product.Stock, product.CreatedAt);
    }
}
