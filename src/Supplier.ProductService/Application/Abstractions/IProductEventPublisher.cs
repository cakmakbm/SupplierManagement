using Shared.Contracts;

namespace Supplier.ProductService.Application.Abstractions;

public interface IProductEventPublisher
{
    Task PublishAsync(ProductCreated message, CancellationToken cancellationToken);
}
