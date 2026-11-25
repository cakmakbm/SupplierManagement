using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Shared.Contracts;
using Supplier.ProductService.Domain.Entities;
using Supplier.ProductService.Infrastructure.Persistence;

namespace Supplier.ProductService.Application.Products;

public sealed class CreateProductHandler(ProductDbContext dbContext)
{
    public async Task<ProductResponse> HandleAsync(CreateProductRequest request, CancellationToken cancellationToken)
    {
        var name = request.Name.Trim();
        var sku = request.SKU.Trim();

        if (await dbContext.Products.AnyAsync(x => x.SupplierId == request.SupplierId && x.SKU == sku, cancellationToken))
            throw new DuplicateProductException(request.SupplierId, sku);

        var product = new Product(request.SupplierId, name, sku, request.Price, request.Stock);
        var message = new ProductCreated(
            Guid.NewGuid(), product.Id, product.SupplierId, product.Name, product.SKU,
            product.Price, product.Stock, product.CreatedAt);

        // Transactional Outbox: the product and its event are saved in one SaveChanges call,
        // which EF Core wraps in a single database transaction. OutboxDispatcher publishes the
        // event to Kafka afterwards, so a Kafka outage can no longer lose the event.
        dbContext.Products.Add(product);
        dbContext.OutboxMessages.Add(new OutboxMessage(
            message.EventId,
            nameof(ProductCreated),
            message.SupplierId.ToString(),
            JsonSerializer.Serialize(message),
            message.OccurredAt));

        try
        {
            await dbContext.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException)
        {
            // Another request may have inserted the same SKU between the check above and this
            // insert, so the unique index rejected ours. Anything else is rethrown.
            if (await ExistsAfterRaceAsync(request.SupplierId, sku, cancellationToken))
                throw new DuplicateProductException(request.SupplierId, sku);
            throw;
        }

        return new ProductResponse(product.Id, product.SupplierId, product.Name, product.SKU,
            product.Price, product.Stock, product.CreatedAt);
    }

    private async Task<bool> ExistsAfterRaceAsync(Guid supplierId, string sku, CancellationToken cancellationToken)
    {
        dbContext.ChangeTracker.Clear();
        return await dbContext.Products.AnyAsync(x => x.SupplierId == supplierId && x.SKU == sku, cancellationToken);
    }
}
